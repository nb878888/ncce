const { Buffer } = require('node:buffer');
const EventEmitter = require('node:events');
/**
 * WebSocket 网络层 - 连接/消息编解码/登录/心跳
 */

const process = require('node:process');
const WebSocket = require('ws');
const { CONFIG } = require('../config/config');
const { createScheduler } = require('../services/scheduler');
const { updateStatusFromLogin, updateStatusGold, updateStatusLevel } = require('../services/status');
const { recordOperation, recordTongQiGift, getTongQiGiftCount } = require('../services/stats');
const { types } = require('./proto');
const { toLong, toNum, syncServerTime, log, logWarn } = require('./utils');
const cryptoWasm = require('./crypto-wasm');

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
};

function getUserState() { return userState; }
function getWsErrorState() { return { ...wsErrorState }; }
function setWsErrorState(code, message) {
    wsErrorState = { code: Number(code) || 0, at: Date.now(), message: message || '' };
}
function clearWsErrorState() {
    wsErrorState = { code: 0, at: 0, message: '' };
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
    return !!obj && Object.prototype.hasOwnProperty.call(obj, key);
}

// ============ 消息编解码 ============
async function encodeMsg(serviceName, methodName, bodyBytes, clientSeqValue) {
    let finalBody = bodyBytes || Buffer.alloc(0);
    if (finalBody.length > 0) {
        finalBody = await cryptoWasm.encryptBuffer(finalBody);
    }
    const msg = types.GateMessage.create({
        meta: {
            service_name: serviceName,
            method_name: methodName,
            message_type: 1,
            client_seq: toLong(clientSeqValue),
            server_seq: toLong(serverSeq),
        },
        body: finalBody,
        auth_token: Buffer.from(`iknowhowyouare${Date.now()}`).toString('base64'),
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

        const sent = sendMsg(serviceName, methodName, bodyBytes, (err, body, meta) => {
            networkScheduler.clear(timeoutKey);
            if (settled) return;
            settled = true;
            if (err) reject(err);
            else resolve({ body, meta });
        });

        if (!sent) {
            networkScheduler.clear(timeoutKey);
            if (!settled) {
                settled = true;
                reject(new Error(`发送失败: ${methodName}`));
            }
        }
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

                updateStatusFromLogin({
                    name: userState.name,
                    level: userState.level,
                    gold: userState.gold,
                    exp: userState.exp,
                });

                log('系统', `登录成功: ${userState.name} (Lv${userState.level})`);

                console.warn('');
                console.warn('========== 登录成功 ==========');
                console.warn(`  GID:    ${userState.gid}`);
                console.warn(`  昵称:   ${userState.name}`);
                console.warn(`  等级:   ${userState.level}`);
                console.warn(`  金币:   ${userState.gold}`);
                if (reply.time_now_millis) {
                    const loginTime = toNum(reply.time_now_millis);
                    const loginTimeMs = loginTime > 1e12 ? loginTime : loginTime * 1000;
                    syncServerTime(loginTimeMs);
                    console.warn(`  时间:   ${new Date(loginTimeMs).toLocaleString()}`);
                }
                console.warn('===============================');
                console.warn('');

                fetchGoldBeanFromBag();
            }

            startHeartbeat();
            startAutoReconnect();
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

// ============ WebSocket 连接 ============
let savedLoginCallback = null;
let savedCode = null;

const DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 MicroMessenger/7.0.20.1781(0x6700143B) NetType/WIFI MiniProgramEnv/Windows WindowsWechat/WMPF WindowsWechat(0x63090a13)';

function connect(code, onLoginSuccess) {
    savedLoginCallback = onLoginSuccess;
    if (code) savedCode = code;
    const url = `${CONFIG.serverUrl}?platform=${CONFIG.platform}&os=${CONFIG.os}&ver=${CONFIG.clientVersion}&code=${savedCode}&openID=`;

    let userAgent = DEFAULT_USER_AGENT;
    let deviceProtocol = null;
    try {
        const store = getStoreModule();
        deviceProtocol = store.getDeviceProtocol();
        if (deviceProtocol && deviceProtocol.enabled && deviceProtocol.userAgent) {
            userAgent = deviceProtocol.userAgent;
        }
    } catch (e) {
        console.error('[DEBUG] 获取设备协议配置失败:', e.message);
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

    ws = new WebSocket(url, {
        headers: {
            'User-Agent': userAgent,
            'Origin': 'https://gate-obt.nqf.qq.com',
        },
    });

    ws.binaryType = 'arraybuffer';

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
        }
    });

    ws.on('error', (err) => {
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
    rejectAllPendingRequests(`请求已中断: ${reason}`);
    networkScheduler.clearAll();
    wsReady = false;
}

function reconnect(newCode) {
    cleanup('主动重连');
    if (ws) {
        ws.removeAllListeners();
        ws.close();
        ws = null;
    }
    userState.gid = 0;
    connect(newCode || savedCode, savedLoginCallback);
}

function getWs() { return ws; }
function isConnected() { return !!(ws && ws.readyState === WebSocket.OPEN && wsReady); }

module.exports = {
    connect, reconnect, cleanup, getWs, isConnected,
    sendMsg, sendMsgAsync,
    getUserState,
    getWsErrorState,
    networkEvents,
    triggerReconnect,
};
