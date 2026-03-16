package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.VoucherOrderDTO;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.concurrent.ListenableFuture;

import javax.annotation.Resource;
import java.util.Collections;

/**
 * <p>
 * Kafka 版本秒杀服务实现类
 * </p>
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;

    // 注入 Kafka 模板用于发送消息
    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    // 【关键修复】使用 @Lazy 延迟注入自己，获取代理对象，完美解决 Kafka 异步线程中 AopContext.currentProxy() 失效的问题
    @Resource
    @Lazy
    private IVoucherOrderService proxy;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    /**
     * 秒杀券下单方法 (生产者)
     * @param voucherId 秒杀券ID
     * @return 订单ID或错误信息
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");

        // 1. 执行 Lua 脚本 (判断库存 + 扣减库存)
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );

        // 2. 判断 Lua 脚本结果
        int r = result.intValue();
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "不允许重复下单");
        }

        // 3. Lua 执行成功，准备发送 Kafka 消息
        VoucherOrderDTO orderDTO = new VoucherOrderDTO(userId, voucherId, orderId);
        String jsonMessage = JSON.toJSONString(orderDTO);

        // 4. 异步发送 Kafka 消息并配置回调 (保证消息不丢失)
        ListenableFuture<SendResult<String, String>> future =
                kafkaTemplate.send("seckill-topic", jsonMessage);

        future.addCallback(sendResult -> {
            log.info("Kafka消息发送成功！订单ID: {}", orderId);
        }, ex -> {
            log.error("Kafka消息发送失败！订单ID: {}", orderId, ex);
        });

        // 5. 快速返回订单ID给前端（无需等待落库完成）
        return Result.ok(orderId);
    }

    /**
     * Kafka 消费者方法：监听秒杀消息并落库
     * @param message Kafka 消息内容 (JSON)
     * @param ack     用于手动提交偏移量
     */
    @KafkaListener(topics = "seckill-topic", groupId = "seckill-group")
    public void onMessage(String message, Acknowledgment ack) {
        try {
            if (StrUtil.isBlank(message)) {
                ack.acknowledge();
                return;
            }

            log.info("接收到 Kafka 消息: {}", message);

            // 1. 反序列化
            VoucherOrderDTO orderDTO = JSON.parseObject(message, VoucherOrderDTO.class);

            // 2. 转换为实体类
            VoucherOrder voucherOrder = new VoucherOrder();
            voucherOrder.setId(orderDTO.getOrderId());
            voucherOrder.setUserId(orderDTO.getUserId());
            voucherOrder.setVoucherId(orderDTO.getVoucherId());

            // 3. 调用原有的加锁下单处理逻辑
            handleVoucherOrder(voucherOrder);

            // 4. 业务【完全成功】后，手动提交 ACK 给 Kafka
            ack.acknowledge();
            log.info("订单落库成功，手动提交 ACK。订单ID: {}", orderDTO.getOrderId());

        } catch (Exception e) {
            log.error("消费秒杀订单消息异常，拒绝提交 ACK 以便重试。消息内容: {}", message, e);
            // 这里不调用 ack.acknowledge()，Kafka 会在稍后重新投递该消息
        }
    }

    /**
     * 处理秒杀订单的加锁逻辑
     * @param voucherOrder 秒杀订单对象
     */
    public void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        // 1. 创建 Redisson 锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);

        // 2. 获取锁
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.error("兜底拦截：不允许重复下单, 用户ID: {}", userId);
            return;
        }

        try {
            // 3. 使用注入的 proxy 调用事务方法，确保事务不失效
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            // 4. 释放锁
            lock.unlock();
        }
    }

    /**
     * 创建秒杀订单 (最终写入数据库的方法)
     * @param voucherOrder 秒杀订单对象
     */
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        long userId = voucherOrder.getUserId();
        long voucherId = voucherOrder.getVoucherId();

        // 1. 一人一单的 DB 层最终防线
        long count = query().eq("user_id", userId)
                .eq("voucher_id", voucherId)
                .count();
        if (count > 0) {
            log.error("DB校验：用户已经购买过一次");
            return;
        }

        // 2. 扣减库存 (乐观锁机制)
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();

        if (!success) {
            log.error("DB校验：库存不足");
            return;
        }

        // 3. 创建订单
        save(voucherOrder);
    }
}