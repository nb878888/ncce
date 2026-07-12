const process = require('node:process');
const _0x5aa05a = {};
_0x5aa05a['serverUrl'] = 'wss://gate-obt.nqf.qq.com/prod/ws';
_0x5aa05a['clientVersion'] = '10.11.1.7_20270425';
_0x5aa05a['platform'] = 'qq';
_0x5aa05a['os'] = 'iOS';

const DEFAULT_SYSTEM_CONFIG = _0x5aa05a;
const CONFIG = {
  'serverUrl': DEFAULT_SYSTEM_CONFIG['serverUrl'],
  'clientVersion': DEFAULT_SYSTEM_CONFIG['clientVersion'],
  'platform': DEFAULT_SYSTEM_CONFIG['platform'],
  'os': DEFAULT_SYSTEM_CONFIG['os'],
  'heartbeatInterval': 0x61a8,
  'farmCheckInterval': 0xbb8,
  'friendCheckInterval': 0x2ee0,
  'farmCheckIntervalMin': 0xbb8,
  'farmCheckIntervalMax': 0x1388,
  'friendCheckIntervalMin': 0x2ee0,
  'friendCheckIntervalMax': 0x3a98,
  // 适配 Render 动态端口，优先读取环境变量
  'adminPort': Number(process.env.ADMIN_PORT || process.env.PORT || 3000),
  'adminPassword': process.env.ADMIN_PASSWORD || '123456'
};

function updateRuntimeConfig(_0x406354) {
  if (_0x406354['serverUrl'] && typeof _0x406354['serverUrl'] === 'string') {
    CONFIG['serverUrl'] = _0x406354['serverUrl'];
  }
  if (_0x406354['clientVersion'] && typeof _0x406354['clientVersion'] === 'string') {
    CONFIG['clientVersion'] = _0x406354['clientVersion'];
  }
  if (_0x406354['platform'] && typeof _0x406354['platform'] === 'string') {
    CONFIG['platform'] = _0x406354['platform'];
  }
  if (_0x406354['os'] && typeof _0x406354['os'] === 'string') {
    CONFIG['os'] = _0x406354['os'];
  }
}

function getRuntimeConfig() {
  const _0x4fabf8 = {};
  _0x4fabf8['serverUrl'] = CONFIG['serverUrl'];
  _0x4fabf8['clientVersion'] = CONFIG['clientVersion'];
  _0x4fabf8['platform'] = CONFIG['platform'];
  _0x4fabf8['os'] = CONFIG['os'];
  return _0x4fabf8;
}

function getDefaultSystemConfig() {
  const _0x514b7 = { ...DEFAULT_SYSTEM_CONFIG };
  return _0x514b7;
}

const _0x3ca015 = {};
_0x3ca015['UNKNOWN'] = 0x0;
_0x3ca015['SEED'] = 0x1;
_0x3ca015['GERMINATION'] = 0x2;
_0x3ca015['SMALL_LEAVES'] = 0x3;
_0x3ca015['LARGE_LEAVES'] = 0x4;
_0x3ca015['BLOOMING'] = 0x5;
_0x3ca015['MATURE'] = 0x6;
_0x3ca015['DEAD'] = 0x7;

const PlantPhase = _0x3ca015;
const PHASE_NAMES = ['未知', '种子', '发芽', '小叶', '大叶', '开花', '成熟', '枯死'];

const _0x1eef06 = {};
_0x1eef06['CONFIG'] = CONFIG;
_0x1eef06['PlantPhase'] = PlantPhase;
_0x1eef06['PHASE_NAMES'] = PHASE_NAMES;
_0x1eef06['updateRuntimeConfig'] = updateRuntimeConfig;
_0x1eef06['getRuntimeConfig'] = getRuntimeConfig;
_0x1eef06['getDefaultSystemConfig'] = getDefaultSystemConfig;

module['exports'] = _0x1eef06;
