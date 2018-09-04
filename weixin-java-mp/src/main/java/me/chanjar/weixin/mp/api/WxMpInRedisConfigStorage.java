package me.chanjar.weixin.mp.api;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;

/**
 * 基于Redis的微信配置provider
 * <pre>
 *    使用说明：本实现仅供参考，并不完整，
 *    比如为减少项目依赖，未加入redis分布式锁的实现，如有需要请自行实现。
 * </pre>
 * @author nickwong
 */
@SuppressWarnings("hiding")
public class WxMpInRedisConfigStorage extends WxMpInMemoryConfigStorage {

  private final static String ACCESS_TOKEN_KEY = "wechat_access_token_";

  private final static String JSAPI_TICKET_KEY = "wechat_jsapi_ticket_";

  private final static String CARDAPI_TICKET_KEY = "wechat_cardapi_ticket_";

  private final static String DISTRIBUTED_ACCESS_TOKEN_LOCK_KEY = "distributed_wechat_access_token_lock_";

  /**
   * lua script 脚本 这是一个分布式限流的lua脚本用在分布式锁上也不会有问题
   */
  private static StringBuffer locksb = new StringBuffer();
  static{
    locksb.append("local times = redis.call('incr',KEYS[1])");
    locksb.append(" if times == 1 then");
    locksb.append(" redis.call('expire',KEYS[1], ARGV[1])");
    locksb.append(" end");
    locksb.append(" if times > tonumber(ARGV[2]) then");
    locksb.append(" return 0");
    locksb.append(" end");
    locksb.append(" return 1");
  }

  /**
   * 使用连接池保证线程安全
   */
  protected final JedisPool jedisPool;

  private String accessTokenKey;

  private String jsapiTicketKey;

  private String cardapiTicketKey;

  private String distributedLockAccessTokenKey;

  public WxMpInRedisConfigStorage(JedisPool jedisPool) {
    this.jedisPool = jedisPool;
  }

  /**
   * 每个公众号生成独有的存储key
   *
   * @param appId
   */
  @Override
  public void setAppId(String appId) {
    super.setAppId(appId);
    this.accessTokenKey = ACCESS_TOKEN_KEY.concat(appId);
    this.jsapiTicketKey = JSAPI_TICKET_KEY.concat(appId);
    this.cardapiTicketKey = CARDAPI_TICKET_KEY.concat(appId);
    this.distributedLockAccessTokenKey = DISTRIBUTED_ACCESS_TOKEN_LOCK_KEY.concat(appId);
  }

  @Override
  public String getAccessToken() {
    try (Jedis jedis = this.jedisPool.getResource()) {
      return jedis.get(this.accessTokenKey);
    }
  }

  @Override
  public boolean isAccessTokenExpired() {
    try (Jedis jedis = this.jedisPool.getResource()) {
      return jedis.ttl(accessTokenKey) < 2;
    }
  }

  @Override
  public synchronized void updateAccessToken(String accessToken, int expiresInSeconds) {
    try (Jedis jedis = this.jedisPool.getResource()) {
      jedis.setex(this.accessTokenKey, expiresInSeconds - 200, accessToken);
      //更新AccessToken成功之后，解锁
      this.redisDistributedUnLock();
    }
  }

  @Override
  public void expireAccessToken() {
    try (Jedis jedis = this.jedisPool.getResource()) {
      jedis.expire(this.accessTokenKey, 0);
    }
  }

  @Override
  public String getJsapiTicket() {
    try (Jedis jedis = this.jedisPool.getResource()) {
      return jedis.get(this.jsapiTicketKey);
    }
  }

  @Override
  public boolean isJsapiTicketExpired() {
    try (Jedis jedis = this.jedisPool.getResource()) {
      return jedis.ttl(this.jsapiTicketKey) < 2;
    }
  }

  @Override
  public synchronized void updateJsapiTicket(String jsapiTicket, int expiresInSeconds) {
    try (Jedis jedis = this.jedisPool.getResource()) {
      jedis.setex(this.jsapiTicketKey, expiresInSeconds - 200, jsapiTicket);
    }
  }

  @Override
  public void expireJsapiTicket() {
    try (Jedis jedis = this.jedisPool.getResource()) {
      jedis.expire(this.jsapiTicketKey, 0);
    }
  }

  @Override
  public String getCardApiTicket() {
    try (Jedis jedis = this.jedisPool.getResource()) {
      return jedis.get(this.cardapiTicketKey);
    }
  }

  @Override
  public boolean isCardApiTicketExpired() {
    try (Jedis jedis = this.jedisPool.getResource()) {
      return jedis.ttl(this.cardapiTicketKey) < 2;
    }
  }

  @Override
  public synchronized void updateCardApiTicket(String cardApiTicket, int expiresInSeconds) {
    try (Jedis jedis = this.jedisPool.getResource()) {
      jedis.setex(this.cardapiTicketKey, expiresInSeconds - 200, cardApiTicket);
    }
  }

  @Override
  public void expireCardApiTicket() {
    try (Jedis jedis = this.jedisPool.getResource()) {
      jedis.expire(this.cardapiTicketKey, 0);
    }
  }

  /**
   * 分布式锁-上锁
   * @return
   */
  public boolean redisDistributedLock(){
    Long lockSuccess = 1L;
    try (Jedis jedis = this.jedisPool.getResource()) {
      List<String> keys = new ArrayList<>();
      keys.add(this.distributedLockAccessTokenKey);
      List<String> args = new ArrayList<>();
      //过期时间 6秒
      args.add("6");
      //次数 (6秒钟内只能执行一次，6秒钟足够更新缓存了)
      args.add("1");
      Object result = jedis.eval(locksb.toString(),keys,args);
      if(lockSuccess.equals(result)){
        return true;
      }
    }
    return false;
  }

  /**
   * 分布式锁-解锁
   * @return
   */
  private void redisDistributedUnLock(){
    try (Jedis jedis = this.jedisPool.getResource()) {
      jedis.del(distributedLockAccessTokenKey);
    }
  }
}
