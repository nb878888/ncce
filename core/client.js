const process = require('node:process');
const path = require('path');
const { CONFIG } = require('./src/config/config');

// ========== Render 适配 ==========
// 强制使用 Render 动态端口
CONFIG.adminPort = Number(process.env.PORT || CONFIG.adminPort || 3000);
// 静态文件路径修正
CONFIG.webDistPath = path.join(__dirname, '../web/dist');

// 全局异常捕获
process.on('uncaughtException', (err) => {
    console.error('[Runtime] 未捕获异常:', err.message);
    console.error(err.stack);
});
process.on('unhandledRejection', (reason) => {
    console.error('[Runtime] 未处理 Promise 拒绝:', reason);
});
// =================================

const {
    startAdminServer,
    emitRealtimeStatus,
    emitRealtimeLog,
    emitRealtimeAccountLog,
} = require('./src/controllers/admin');
const { createRuntimeEngine } = require('./src/runtime/runtime-engine');
const { createModuleLogger } = require('./src/services/logger');
const { verifyAndRun } = require('./src/services/license');
const mainLogger = createModuleLogger('main');

const isWorkerProcess = process.env.FARM_WORKER === '1';

async function bootstrap() {
    if (isWorkerProcess) {
        require('./src/core/worker');
        return;
    }

    const licenseValid = await verifyAndRun();
    if (!licenseValid) {
        console.error('');
        console.error('[错误] 授权验证失败，程序即将退出');
        console.error('');
        process.exit(1);
        return;
    }

    // ========== 关键修复：Runtime 引擎初始化时强制绑定 startAccount 方法 ==========
    const runtimeEngine = createRuntimeEngine({
        processRef: process,
        mainEntryPath: __filename,
        startAdminServer,
        onStatusSync: (accountId, status) => {
            emitRealtimeStatus(accountId, status);
        },
        onLog: (entry, accountId) => {
            if (accountId && entry) {
                entry.accountId = accountId;
            }
            emitRealtimeLog(entry);
        },
        onAccountLog: (entry) => {
            emitRealtimeAccountLog(entry);
        },
    });

    // 确保 provider.startAccount 存在（兼容旧版本代码）
    if (!runtimeEngine.provider || typeof runtimeEngine.provider.startAccount !== 'function') {
        mainLogger.warn('Runtime provider.startAccount 未初始化，强制修复');
        // 手动挂载兼容方法
        runtimeEngine.provider = runtimeEngine.provider || {};
        runtimeEngine.provider.startAccount = async (accountId) => {
            mainLogger.info(`[兼容修复] 调用 startAccount(${accountId})`);
            // 调用原始启动逻辑
            return runtimeEngine.startAccount(accountId);
        };
    }

    runtimeEngine.start({
        startAdminServer: true,
        autoStartAccounts: false,
    }).catch((err) => {
        mainLogger.error('runtime bootstrap failed', { error: err && err.message ? err.message : String(err) });
    });
}

bootstrap().catch((err) => {
    console.error('Bootstrap failed:', err);
    process.exit(1);
});
