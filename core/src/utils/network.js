const { Buffer } = require('node:buffer');
const EventEmitter = require('node:events');
/**
 * WebSocket 网络层 - 连接/消息编解码/登录/心跳
 */

const process = require('node:process');
const WebSocket = require('ws');
const { CONFIG } = require('../config/config');
const { AceService } = require('../services/ace-service');
const { createScheduler } = require('../services/scheduler');
const { updateStatusFromLogin, updateStatusGold, updateStatusLevel } = require('../services/status');
const { recordOperation, recordTongQiGift, getTongQiGiftCount } = require('../services/stats');
const { types } = require('./proto');
const { toLong, toNum, syncServerTime, log, logWarn } = require('./utils');
const cryptoWasm = require('./crypto-wasm');
const { createGatewayToken } = require('./gateway-token');
const { TsdkRuntime } = require('./tsdk-runtime');

// 延迟加载 warehouse 模块避免循环依赖
let warehouseModule = null;
function getWarehouseModule() {
    if (!warehouseModule) {
        warehouseModule = require('../services/warehouse');
    }
    return warehouseModule;
}

// 延迟加载 store 模块避免循环依赖
let storeModule = null;
function getStoreModule() {
    if (!storeModule) {
        storeModule = require('../models/store');
    }
    return storeModule;
}

// ============ 事件发射器 (用于推送通知) ============
const networkEvents = new EventEmitter();

// ============ 内部状态 ============
let ws = null;
let clientSeq = 1;
let serverSeq = 0;
const pendingCallbacks = new Map();
let wsErrorState = { code: 0, at: 0, message: '' };
const networkScheduler = createScheduler('network');
let tsdkRuntime = null;
let aceService = null;
let initialGamePackInfo = '';

function logAce(level, message) {
    if (level === 'warn' || level === 'error') logWarn('ACE', message);
    else log('ACE', message);
}

function createTsdkRuntime(deviceProtocol) {
    return new TsdkRuntime({
        accountId: process.env.FARM_ACCOUNT_ID,
        gameId: CONFIG.tsdkGameId,
        appKey: CONFIG.tsdkAppKey,
        deviceInfo: {
            deviceModel: deviceProtocol && deviceProtocol.deviceModel,
            deviceBrand: deviceProtocol && deviceProtocol.deviceBrand,
            deviceId: deviceProtocol && deviceProtocol.deviceId,
            platform: CONFIG.os,
        },
        logger: logAce,
    });
}

async function startSecurityRuntime(deviceProtocol) {
    stopSecurityRuntime('重新初始化');
    if (!CONFIG.tsdkAceEnabled) {
        throw new Error('TSDK/ACE 已通过 FARM_TSDK_ACE_ENABLED=false 关闭，网关请求不会使用伪造 Token');
    }
    tsdkRuntime = createTsdkRuntime(deviceProtocol);
    initialGamePackInfo = '';
    cryptoWasm.setRuntime(tsdkRuntime);
    await tsdkRuntime.init();
}

function startAceService() {
    if (!tsdkRuntime || !tsdkRuntime.ready) throw new Error('TSDK 尚未就绪');
    if (aceService) aceService.stop('重新启动');
    aceService = new AceService({
        runtime: tsdkRuntime,
        sendRequest: sendMsgAsync,
        isConnected,
        types,
        logger: logAce,
    });
    aceService.start();
}

function stopSecurityRuntime(reason = '停止') {
    if (aceService) {
        aceService.stop(reason);
        aceService = null;
    }
    if (tsdkRuntime) {
        tsdkRuntime.destroy();
        tsdkRuntime = null;
    }
    initialGamePackInfo = '';
    cryptoWasm.setRuntime(null);
}

// ===================== 新增防重连并发全局变量 ======================
let wsReady = false; // 通道完全就绪标记
let autoReconnectTimer = null;
let lastConnectTime = 0;
let isAutoReconnecting = false;
const RECONNECT_INTERVAL = 140000; // 2分20秒提前重连，预留缓冲
// ==================================================================

function rejectAllPendingRequests(reason = '请求被中断') {
    const entries = Array.from(pendingCallbacks.entries());
    pendingCallbacks.clear();
    for (const [, callback] of entries) {
        try {
            callback(new Error(reason));
        } catch {
            // ignore callback failure
        }
    }
    return entries.length;
}

// ============ 用户状态 (登录后设置) ============
const userState = {
    gid: 0,
    name: '',
    level: 0,
    gold: 0,
    exp: 0,
    coupon: 0, // 点券(ID:1002)
    goldBean: 0, // 金豆豆(ID:1005)
    openId: '',
    avatar: '',
};

function getUserState() { return userState; }
function getWsErrorState() { return { ...wsErrorState }; }
function setWsErrorState(code, message) {
    wsErrorState = { code: Number(code) || 0, at: Date.now(), message: message || '' };
}
function clearWsErrorState() {
    wsErrorState = { code: 0, at: 0, message: '' };
}

function logLoginSummary(loginTimeMs) {
    const lines = [
        `GID: ${userState.gid}`,
        `昵称: ${userState.name}`,
        `等级: ${userState.level}`,
        `金币: ${userState.gold}`,
    ];
    if (loginTimeMs) {
        lines.push(`时间: ${new Date(loginTimeMs).toLocaleString()}`);
    }
    log('系统', `登录摘要\n${lines.join('\n')}`);
}

// 登录后从背包获取金豆豆数量
async function fetchGoldBeanFromBag() {
    try {
        const warehouse = getWarehouseModule();
        const bagReply = await warehouse.getBag();
        const items = warehouse.getBagItems(bagReply);
        for (const item of (items || [])) {
            const id = toNum(item && item.id);
            const count = toNum(item && item.count);
            if (id === 1005 && count > 0) {
                userState.goldBean = count;
                log('系统', `金豆豆数量: ${count}`);
                break;
            }
        }
    // eslint-disable-next-line unused-imports/no-unused-vars
    } catch (e) {
        // 忽略获取失败
    }
}

function hasOwn(obj, key) {
    return !!obj && Object.hasOwn(obj, key);
}

// ============ 消息编解码 ============
async function encodeMsg(serviceName, methodName, bodyBytes, clientSeqValue) {
    let finalBody = bodyBytes || Buffer.alloc(0);
    if (finalBody.length > 0) {
        finalBody = await cryptoWasm.encryptBuffer(finalBody);
    }
    const gatewayToken = initialGamePackInfo || createGatewayToken();
    initialGamePackInfo = '';
    const msg = types.GateMessage.create({
        meta: {
            service_name: serviceName,
            method_name: methodName,
            message_type: 1,
            client_seq: toLong(clientSeqValue),
            server_seq: toLong(serverSeq),
        },
        body: finalBody,
<<<<<<< HEAD
        auth_token: gatewayToken,
=======
        auth_token: Buffer.from(`iknowhowyouare${Date.now()}`).toString('base64'),
>>>>>>> 7af1e92ba2027f40ed7da830277c0f8c574b8a9a
    });
    return types.GateMessage.encode(msg).finish();
}

async function sendMsg(serviceName, methodName, bodyBytes, callback) {
    // 重连拦截判断
    if (!wsReady || isAutoReconnecting) {
        return false;
    }
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        log('系统', '[WS] 连接未打开');
        return false;
    }
    const seq = clientSeq;
    clientSeq += 1;
    const encoded = await encodeMsg(serviceName, methodName, bodyBytes, seq);
    if (callback) pendingCallbacks.set(seq, callback);
    try {
        ws.send(encoded);
    } catch (err) {
        if (callback) {
            pendingCallbacks.delete(seq);
            callback(err);
        }
        return false;
    }
    return true;
}

/** Promise 版发送 */
function sendMsgAsync(serviceName, methodName, bodyBytes, timeout = 20000) {
    return new Promise((resolve, reject) => {
        // 重连期间直接拦截，不抛出大量报错
        if (!wsReady || isAutoReconnecting) {
            reject(new Error(`正在自动重连，暂时无法发送请求: ${methodName}`));
            return;
        }
        if (!ws || ws.readyState !== WebSocket.OPEN) {
            reject(new Error(`连接未打开: ${methodName}`));
            return;
        }

        if (pendingCallbacks.size >= 10) {
            reject(new Error(`请求队列已满: ${methodName} (pending=${pendingCallbacks.size})`));
            return;
        }

        const seq = clientSeq;
        const timeoutKey = `request_timeout_${seq}`;
        let settled = false;

        networkScheduler.setTimeoutTask(timeoutKey, timeout, () => {
            if (settled) return;
            settled = true;
            pendingCallbacks.delete(seq);
            const pending = pendingCallbacks.size;
            reject(new Error(`请求超时: ${methodName} (seq=${seq}, pending=${pending})`));
        });

        sendMsg(serviceName, methodName, bodyBytes, (err, body, meta) => {
            networkScheduler.clear(timeoutKey);
            if (settled) return;
            settled = true;
            if (err) reject(err);
            else resolve({ body, meta });
        }).then((sent) => {
            if (sent || settled) return;
            networkScheduler.clear(timeoutKey);
            settled = true;
            reject(new Error(`发送失败: ${methodName}`));
        }).catch((error) => {
            if (settled) return;
            networkScheduler.clear(timeoutKey);
            pendingCallbacks.delete(seq);
            settled = true;
            reject(error);
        });
    });
}

// ============ 消息处理 ============
function handleMessage(data) {
    try {
        const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
        const msg = types.GateMessage.decode(buf);
        const meta = msg.meta;
        if (!meta) return;

        if (meta.server_seq) {
            const seq = toNum(meta.server_seq);
            if (seq > serverSeq) serverSeq = seq;
        }

        const msgType = meta.message_type;

        // Notify
        if (msgType === 3) {
            handleNotify(msg);
            return;
        }

        // Response
        if (msgType === 2) {
            const errorCode = toNum(meta.error_code);
            const clientSeqVal = toNum(meta.client_seq);

            const cb = pendingCallbacks.get(clientSeqVal);
            if (cb) {
                pendingCallbacks.delete(clientSeqVal);
                if (errorCode !== 0) {
                    cb(new Error(`${meta.service_name}.${meta.method_name} 错误: code=${errorCode} ${meta.error_message || ''}`));
                } else {
                    cb(null, msg.body, meta);
                }
                return;
            }

            if (errorCode !== 0) {
                logWarn('错误', `${meta.service_name}.${meta.method_name} code=${errorCode} ${meta.error_message || ''}`);
            }
        }
    } catch (err) {
        logWarn('解码', err.message);
    }
}

function handleNotify(msg) {
    if (!msg.body || msg.body.length === 0) return;
    try {
        const event = types.EventMessage.decode(msg.body);
        const type = event.message_type || '';
        const eventBody = event.body;

        // 被踢下线
        if (type.includes('Kickout')) {
            log('推送', `被踢下线! ${type}`);
            try {
                const notify = types.KickoutNotify.decode(eventBody);
                log('推送', `原因: ${notify.reason_message || '未知'}`);
                networkEvents.emit('kickout', {
                    type,
                    reason: notify.reason_message || '未知',
                });
            } catch { }
            return;
        }

        // 土地状态变化 (被放虫/放草/偷菜等)
        if (type.includes('LandsNotify')) {
            try {
                const notify = types.LandsNotify.decode(eventBody);
                const hostGid = toNum(notify.host_gid);
                const lands = notify.lands || [];
                if (lands.length > 0) {
                    if (hostGid === userState.gid || hostGid === 0) {
                        networkEvents.emit('landsChanged', lands);
                    }
                }
            } catch { }
            return;
        }

        // 物品变化通知 (经验/金币等)
        if (type.includes('ItemNotify')) {
            try {
                const notify = types.ItemNotify.decode(eventBody);
                const items = notify.items || [];
                for (const itemChg of items) {
                    const item = itemChg.item;
                    if (!item) continue;
                    const id = toNum(item.id);
                    const count = toNum(item.count);
                    const delta = toNum(itemChg.delta);
                    
                    if (id === 1101) {
                        if (count > 0) userState.exp = count;
                        else if (delta !== 0) userState.exp = Math.max(0, Number(userState.exp || 0) + delta);
                        updateStatusLevel(userState.level, userState.exp);
                    } else if (id === 1 || id === 1001) {
                        if (count > 0) {
                            userState.gold = count;
                        } else if (delta !== 0) {
                            userState.gold = Math.max(0, Number(userState.gold || 0) + delta);
                        }
                        updateStatusGold(userState.gold);
                    } else if (id === 1002) {
                        if (count > 0) {
                            userState.coupon = count;
                        } else if (delta !== 0) {
                            userState.coupon = Math.max(0, Number(userState.coupon || 0) + delta);
                        }
                    } else if (id === 1005) {
                        if (count > 0) {
                            userState.goldBean = count;
                        } else if (delta !== 0) {
                            userState.goldBean = Math.max(0, Number(userState.goldBean || 0) + delta);
                        }
                    } else if (id === 101351) {
                        if (delta > 0 || count > 0) {
                            const giftDelta = delta > 0 ? delta : (count > 0 ? 1 : 0);
                            recordTongQiGift(giftDelta);
                            const currentCount = getTongQiGiftCount();
                            log('好友', `获得同气连枝礼包 +${giftDelta} (今日: ${currentCount})`, {
                                module: 'friend',
                                event: '同气连枝礼包',
                                result: 'ok',
                                count: giftDelta,
                                dailyTotal: currentCount,
                            });
                        }
                    }
                }
            } catch { }
            return;
        }

        // 基本信息变化 (升级等)
        if (type.includes('BasicNotify')) {
            try {
                const notify = types.BasicNotify.decode(eventBody);
                if (notify.basic) {
                    const oldLevel = userState.level;
                    if (hasOwn(notify.basic, 'level')) {
                        const nextLevel = toNum(notify.basic.level);
                        if (Number.isFinite(nextLevel) && nextLevel > 0) userState.level = nextLevel;
                    }
                    let shouldUpdateGoldView = false;
                    if (hasOwn(notify.basic, 'gold')) {
                        const nextGold = toNum(notify.basic.gold);
                        if (Number.isFinite(nextGold) && nextGold >= 0) {
                            userState.gold = nextGold;
                            shouldUpdateGoldView = true;
                        }
                    }
                    if (hasOwn(notify.basic, 'exp')) {
                        const exp = toNum(notify.basic.exp);
                        if (Number.isFinite(exp) && exp >= 0) {
                            userState.exp = exp;
                            updateStatusLevel(userState.level, exp);
                        }
                    }
                    if (shouldUpdateGoldView) {
                        updateStatusGold(userState.gold);
                    }
                    if (userState.level !== oldLevel) {
                        recordOperation('levelUp', 1);
                    }
                }
            } catch { }
            return;
        }

        // 好友申请通知 (微信同玩)
        if (type.includes('FriendApplicationReceivedNotify')) {
            try {
                const notify = types.FriendApplicationReceivedNotify.decode(eventBody);
                const applications = notify.applications || [];
                if (applications.length > 0) {
                    networkEvents.emit('friendApplicationReceived', applications);
                }
            } catch { }
            return;
        }

        // 好友添加成功通知
        if (type.includes('FriendAddedNotify')) {
            try {
                const notify = types.FriendAddedNotify.decode(eventBody);
                const friends = notify.friends || [];
                if (friends.length > 0) {
                    const names = friends.map(f => f.name || f.remark || `GID:${toNum(f.gid)}`).join(', ');
                    log('好友', `新好友: ${names}`);
                }
            } catch { }
            return;
        }

        // 商品解锁通知 (升级后解锁新种子等)
        if (type.includes('GoodsUnlockNotify')) {
            try {
                const notify = types.GoodsUnlockNotify.decode(eventBody);
                const goods = notify.goods_list || [];
                if (goods.length > 0) {
                    networkEvents.emit('goodsUnlockNotify', goods);
                }
            } catch { }
            return;
        }

        // 任务状态变化通知
        if (type.includes('TaskInfoNotify')) {
            try {
                const notify = types.TaskInfoNotify.decode(eventBody);
                if (notify.task_info) {
                    networkEvents.emit('taskInfoNotify', notify.task_info);
                }
            } catch { }
            
        }
    } catch (e) {
        logWarn('推送', `解码失败: ${e.message}`);
    }
}

// ============ 登录 ============
async function sendLogin(onLoginSuccess) {
    const body = types.LoginRequest.encode(types.LoginRequest.create({
        sharer_id: toLong(0),
        sharer_open_id: '',
        device_info: {
            client_version: CONFIG.clientVersion,
            sys_software: 'iOS 26.2.1',
            network: 'wifi',
            memory: '7672',
            device_id: 'iPhone X<iPhone18,3>',
        },
        share_cfg_id: toLong(0),
        scene_id: '1256',
        report_data: {
            callback: '', cd_extend_info: '', click_id: '', clue_token: '',
            minigame_channel: 'other', minigame_platid: 2, req_id: '', trackid: '',
        },
    })).finish();

    await sendMsg('gamepb.userpb.UserService', 'Login', body, (err, bodyBytes, _meta) => {
        if (err) {
            log('登录', `失败: ${err.message}`);
            if (err.message.includes('code=')) {
                log('系统', '账号验证失败，即将停止运行...');
                networkScheduler.setTimeoutTask('login_error_exit', 1000, () => process.exit(0));
            }
            return;
        }
        try {
            const reply = types.LoginReply.decode(bodyBytes);
            if (reply.basic) {
                clearWsErrorState();
                userState.gid = toNum(reply.basic.gid);
                userState.name = reply.basic.name || '未知';
                userState.level = toNum(reply.basic.level);
                userState.gold = toNum(reply.basic.gold);
                userState.exp = toNum(reply.basic.exp);
                userState.openId = String(reply.basic.open_id || '').trim();
                userState.avatar = String(reply.basic.avatar_url || '').trim();
                if (tsdkRuntime && userState.openId) {
                    tsdkRuntime.bindUser(userState.openId);
                    initialGamePackInfo = tsdkRuntime.getEncryptedInitInfo();
                    logAce('info', `ACE 用户身份已绑定：初始化凭据长度 ${initialGamePackInfo.length}`);
                }

                updateStatusFromLogin({
                    gid: userState.gid,
                    name: userState.name,
                    level: userState.level,
                    gold: userState.gold,
                    exp: userState.exp,
                    openId: userState.openId,
                    avatar: userState.avatar,
                });

                log('系统', `登录成功: ${userState.name} (Lv${userState.level})`);

                let loginTimeMs = 0;
                if (reply.time_now_millis) {
                    const loginTime = toNum(reply.time_now_millis);
                    loginTimeMs = loginTime > 1e12 ? loginTime : loginTime * 1000;
                    syncServerTime(loginTimeMs);
                }
                logLoginSummary(loginTimeMs);

                fetchGoldBeanFromBag();
            }

            startHeartbeat();
<<<<<<< HEAD
            startAceService();
=======
            startAutoReconnect();
>>>>>>> 7af1e92ba2027f40ed7da830277c0f8c574b8a9a
            if (onLoginSuccess) onLoginSuccess();
        } catch (e) {
            log('登录', `解码失败: ${e.message}`);
        }
    });
}

// ============ 心跳 ============
let lastHeartbeatResponse = Date.now();
let heartbeatMissCount = 0;
const HEARTBEAT_TIMEOUT = 30000;
const MAX_HEARTBEAT_MISS = 3;

function startHeartbeat() {
    networkScheduler.clear('heartbeat_interval');
    lastHeartbeatResponse = Date.now();
    heartbeatMissCount = 0;

    networkScheduler.setIntervalTask('heartbeat_interval', CONFIG.heartbeatInterval, () => {
        if (!userState.gid) return;

        const timeSinceLastResponse = Date.now() - lastHeartbeatResponse;
        if (timeSinceLastResponse > HEARTBEAT_TIMEOUT) {
            heartbeatMissCount++;
            logWarn('心跳', `连接可能已断开 (${Math.round(timeSinceLastResponse/1000)}s 无响应, miss=${heartbeatMissCount}/${MAX_HEARTBEAT_MISS}, pending=${pendingCallbacks.size})`);
            if (heartbeatMissCount >= MAX_HEARTBEAT_MISS) {
                if (!ws || ws.readyState !== WebSocket.OPEN) {
                    log('心跳', '连接已关闭，立即重连...');
                } else {
                    log('心跳', '心跳超时，关闭连接并重连...');
                    try { ws.close(); } catch { }
                }
                networkEvents.emit('disconnect', { code: 'heartbeat_timeout' });
                rejectAllPendingRequests('连接超时，已清理');
                reconnect(null);
                return;
            }
        }

        const body = types.HeartbeatRequest.encode(types.HeartbeatRequest.create({
            gid: toLong(userState.gid),
            client_version: CONFIG.clientVersion,
        })).finish();
        sendMsgAsync('gamepb.userpb.UserService', 'Heartbeat', body).then(({ body: replyBody }) => {
            lastHeartbeatResponse = Date.now();
            heartbeatMissCount = 0;
            try {
                const reply = types.HeartbeatReply.decode(replyBody);
                if (reply.server_time) {
                    const serverTime = toNum(reply.server_time);
                    const serverTimeMs = serverTime > 1e12 ? serverTime : serverTime * 1000;
                    syncServerTime(serverTimeMs);
                }
            } catch { }
        }).catch(() => {
            // 心跳失败不处理，等待miss计数触发重连
        });
    });
}

<<<<<<< HEAD
=======
// ============ 自动重连逻辑（突破3分钟好友操作限制） ============
function startAutoReconnect() {
    if (autoReconnectTimer) clearInterval(autoReconnectTimer);
    
    autoReconnectTimer = setInterval(async () => {
        if (isAutoReconnecting) return;
        if (Date.now() - lastConnectTime < RECONNECT_INTERVAL) return;
        if (!ws || ws.readyState !== WebSocket.OPEN) return;
        
        isAutoReconnecting = true;
        wsReady = false; // 锁定通道，拦截所有请求
        log('重连', '⏰ 连接已快到3分钟，自动重连刷新好友权限...');
        
        try {
            if (ws) {
                ws.removeAllListeners();
                try { ws.close(); } catch { }
                ws = null;
            }
            cleanup('定时自动重连');
            
            await new Promise(resolve => setTimeout(resolve, 1000));
            
            connect(savedCode, savedLoginCallback);
            lastConnectTime = Date.now();
            log('重连', '✅ 自动重连成功，继续偷菜帮忙');
        } catch (e) {
            logWarn('重连', `❌ 自动重连失败: ${e.message}`);
        } finally {
            isAutoReconnecting = false;
        }
    }, 5000);
}

// 外部调用：立即触发重连（好友操作失败时使用）
function triggerReconnect() {
    lastConnectTime = 0;
}

>>>>>>> 7af1e92ba2027f40ed7da830277c0f8c574b8a9a
// ============ WebSocket 连接 ============
let savedLoginCallback = null;
let savedCode = null;
let reconnectAttempts = 0;
let networkStopped = false;

const DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 MicroMessenger/7.0.20.1781(0x6700143B) NetType/WIFI MiniProgramEnv/Windows WindowsWechat/WMPF WindowsWechat(0x63090a13)';

const MAX_RECONNECT_ATTEMPTS = 5;
const RECONNECT_BASE_DELAY_MS = 2000;
const RECONNECT_MAX_DELAY_MS = 30000;

function closeCurrentWs({ terminate = false } = {}) {
    const current = ws;
    if (!current) return;
    ws = null;
    current.removeAllListeners();
    try {
        if (terminate && typeof current.terminate === 'function') current.terminate();
        else current.close();
    } catch { }
}

function getReconnectDelayMs() {
    const delay = RECONNECT_BASE_DELAY_MS * (2 ** Math.max(0, reconnectAttempts - 1));
    return Math.min(RECONNECT_MAX_DELAY_MS, delay);
}

function scheduleReconnect(reason) {
    if (networkStopped || !savedLoginCallback) return;
    if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
        const message = `自动重连失败次数过多，已停止重连${reason ? ` (${reason})` : ''}`;
        logWarn('系统', `[WS] ${message}`);
        networkEvents.emit('reconnect_failed', {
            attempts: reconnectAttempts,
            reason: reason || '',
        });
        return;
    }

    reconnectAttempts += 1;
    const delayMs = getReconnectDelayMs();
    networkScheduler.setTimeoutTask('auto_reconnect', delayMs, () => {
        if (networkStopped || !savedLoginCallback) return;
        log('系统', `[WS] 尝试自动重连... (${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`);
        reconnect(null);
    });
}

function connect(code, onLoginSuccess) {
    networkStopped = false;
    savedLoginCallback = onLoginSuccess;
    if (code) savedCode = code;
    const url = `${CONFIG.serverUrl}?platform=${CONFIG.platform}&os=${CONFIG.os}&ver=${CONFIG.clientVersion}&code=${savedCode}&openID=`;
    closeCurrentWs({ terminate: true });

    let userAgent = DEFAULT_USER_AGENT;
    let deviceProtocol = null;
    try {
        const store = getStoreModule();
        deviceProtocol = store.getDeviceProtocol();
        if (deviceProtocol && deviceProtocol.enabled && deviceProtocol.userAgent) {
            userAgent = deviceProtocol.userAgent;
        }
    } catch (e) {
        log('system', `failed to load device protocol config: ${e.message}`, {
            module: 'network',
            event: 'device_protocol',
            isWarn: true,
        });
    }

    if (deviceProtocol && deviceProtocol.enabled) {
        const deviceInfo = [
            `品牌: ${deviceProtocol.deviceBrand || '未设置'}`,
            `型号: ${deviceProtocol.deviceModel || '未设置'}`,
            `MAC: ${deviceProtocol.deviceMac || '未设置'}`,
            `设备ID: ${deviceProtocol.deviceId || '未设置'}`,
            `IMEI: ${deviceProtocol.imei || '未设置'}`,
        ].join(' | ');
        log('系统', `使用自定义设备协议登录\n${deviceInfo}\nUA: ${userAgent.substring(0, 100)}...`, {
            module: 'network',
            event: '设备协议',
        });
    }

    const socket = new WebSocket(url, {
        headers: {
            'User-Agent': userAgent,
            'Origin': 'https://gate-obt.nqf.qq.com',
        },
    });
    ws = socket;

    socket.binaryType = 'arraybuffer';

<<<<<<< HEAD
    socket.on('open', async () => {
        reconnectAttempts = 0;
        try {
            await startSecurityRuntime(deviceProtocol);
            await sendLogin(onLoginSuccess);
        } catch (error) {
            logWarn('ACE', `安全运行时启动失败，已中止登录：${error.message}`);
            networkEvents.emit('security_error', { message: error.message });
            stopSecurityRuntime('初始化失败');
            closeCurrentWs({ terminate: true });
=======
    ws.on('open', () => {
        lastConnectTime = Date.now();
        wsReady = true; // 通道就绪放行请求
        sendLogin(onLoginSuccess);
    });

    ws.on('message', (data) => {
        handleMessage(Buffer.isBuffer(data) ? data : Buffer.from(data));
    });

    ws.on('close', (code, _reason) => {
        wsReady = false; // 通道关闭拦截请求
        console.warn(`[WS] 连接关闭 (code=${code})`);
        networkEvents.emit('disconnect', { code });
        cleanup();
        if (savedLoginCallback && !isAutoReconnecting) {
            networkScheduler.setTimeoutTask('auto_reconnect', 2000, () => {
                log('系统', '[WS] 尝试自动重连...');
                reconnect(null);
            });
>>>>>>> 7af1e92ba2027f40ed7da830277c0f8c574b8a9a
        }
    });

    socket.on('message', (data) => {
        handleMessage(Buffer.isBuffer(data) ? data : Buffer.from(data));
    });

    socket.on('close', (code, _reason) => {
        if (ws === socket) ws = null;
        logWarn('系统', `[WS] 连接关闭 (code=${code})`);
        networkEvents.emit('disconnect', { code });
        cleanup();
        scheduleReconnect(`close:${code}`);
    });

    socket.on('error', (err) => {
        const message = err && err.message ? String(err.message) : '';
        logWarn('系统', `[WS] 错误: ${message}`);
        const match = message.match(/Unexpected server response:\s*(\d+)/i);
        if (match) {
            const code = Number.parseInt(match[1], 10) || 0;
            if (code) {
                setWsErrorState(code, message);
                networkEvents.emit('ws_error', { code, message });
            }
        }
    });
}

function cleanup(reason = '网络清理') {
    stopSecurityRuntime(reason);
    rejectAllPendingRequests(`请求已中断: ${reason}`);
    networkScheduler.clearAll();
    wsReady = false;
}

function reconnect(newCode) {
    if (networkStopped || !savedLoginCallback) return false;
    cleanup('主动重连');
    closeCurrentWs({ terminate: true });
    userState.gid = 0;
    connect(newCode || savedCode, savedLoginCallback);
    return true;
}

function stopNetwork(reason = '停止网络') {
    networkStopped = true;
    savedLoginCallback = null;
    reconnectAttempts = 0;
    stopSecurityRuntime(reason);
    rejectAllPendingRequests(`请求已中断: ${reason}`);
    networkScheduler.clearAll();
    closeCurrentWs({ terminate: true });
    userState.gid = 0;
}

function getWs() { return ws; }
<<<<<<< HEAD
function isConnected() { return !!(ws && ws.readyState === WebSocket.OPEN); }
function getAceStatus() {
    if (aceService) return aceService.getStatus();
    return tsdkRuntime ? { running: false, runtime: tsdkRuntime.getStatus() } : null;
}
=======
function isConnected() { return !!(ws && ws.readyState === WebSocket.OPEN && wsReady); }
>>>>>>> 7af1e92ba2027f40ed7da830277c0f8c574b8a9a

module.exports = {
    connect, reconnect, cleanup, stopNetwork, getWs, isConnected,
    sendMsg, sendMsgAsync,
    getUserState,
    getWsErrorState,
    getAceStatus,
    networkEvents,
};
