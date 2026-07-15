const friendApi = require('./friend-api');
const { getOperationLimits } = require('./friend-operation-limits');
const {
  getFriendsList,
  getFriendLandsDetail,
  getFriendDogInfo,
  batchGetFriendDogInfo,
  fetchFriendsDogInfo,
} = require('./friend-land-analyzer');
const { doFriendOperation } = require('./friend-visit');
const {
  checkFriends,
  startFriendCheckLoop,
  stopFriendCheckLoop,
  refreshFriendCheckLoop,
  runBadOnceOnStartup,
  isHelpExpLimitReached,
  clearFriendsListCache,
  syncFriendsFromGids,
} = require('./friend-orchestrator');

module.exports = {
  checkFriends,
  startFriendCheckLoop,
  stopFriendCheckLoop,
  refreshFriendCheckLoop,
  runBadOnceOnStartup,
  isHelpExpLimitReached,
  getOperationLimits,
  getFriendsList,
  getFriendLandsDetail,
  doFriendOperation,
  clearFriendsListCache,
  getFriendDogInfo,
  batchGetFriendDogInfo,
  syncFriendsFromGids,
  fetchFriendsDogInfo,
  delFriend: friendApi.delFriend,
};
