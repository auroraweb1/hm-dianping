package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
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

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    // 阻塞队列，用于存储秒杀订单任务
    private final BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    // 线程，用于处理秒杀订单任务
    public static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();


    /**
     * 初始化方法，在Spring容器启动时执行
     * 用于启动一个线程来处理秒杀订单任务
     */
    @PostConstruct
    private void init() {
        // 启动线程处理秒杀订单任务
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }


    /**
     * 处理秒杀订单任务的线程类
     * 该类实现了Runnable接口，用于从消息队列中获取订单信息并处理
     */
    private class VoucherOrderHandler implements Runnable {
        private static final String queueName = "stream.orders";
        @Override
        public void run() {
            while (true) {
                try {
                    // 1. 获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAM streams.order >
                    List<MapRecord<String, Object, Object>> read = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    // 2. 判断消息是否获取成功
                    if (read == null || read.isEmpty()) {
                        // 如果没有消息，继续下一次循环
                        continue;
                    }
                    // 3. 解析消息中的订单信息
                    MapRecord<String, Object, Object> record = read.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 4. 如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    // 5. 确认消息已经被消费 XACK streams.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    handlePendingList();
                }
            }
        }


        /**
         * 处理pending-list中的订单信息
         * 该方法用于处理由于某些原因未能成功处理的订单信息
         */
        private void handlePendingList() {
            while (true) {
                try {
                    // 1. 获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 STREAM streams.order 0
                    List<MapRecord<String, Object, Object>> read = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    // 2. 判断消息是否获取成功
                    if (read == null || read.isEmpty()) {
                        // 如果没有消息，说明pending-list没有异常消息，结束循环
                        break;
                    }
                    // 3. 解析消息中的订单信息
                    MapRecord<String, Object, Object> record = read.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 4. 如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    // 5. 确认消息已经被消费 XACK streams.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理pending-list中的订单信息失败", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
    }


    /**
     * 处理秒杀订单任务
     * @param voucherOrder 秒杀订单对象
     */
    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        // 1. 获取用户ID
        Long userId = voucherOrder.getUserId();
        // 2. 创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        // 3. 获取锁
        boolean isLock = lock.tryLock();
        // 4. 判断是否获取锁成功
        if (!isLock) {
            // 获取锁失败，返回错误信息
            log.error("不允许重复下单");
            return;
        }
        try {
            proxy.createVoucherOrder(voucherOrder);
        }finally {
            // 释放锁
            lock.unlock();
        }
    }


    private IVoucherOrderService proxy;
    /**
     * 秒杀券下单方法
     * @param voucherId 秒杀券ID
     * @return 订单ID或错误信息
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 1. 执行Lua脚本
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );

        // 2. 判断结果
        int r = result.intValue();
        if (r != 0) {
            // 2.1 如果结果不为0，说明秒杀失败
            return Result.fail(r == 1 ? "库存不足" : "不允许重复下单");
        }

        // 3. 获取代理对象（事务）
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        // 4. 返回订单ID
        return Result.ok(orderId);
    }


//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        // 1. 执行Lua脚本
//        Long userId = UserHolder.getUser().getId();
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString()
//        );
//        // 2. 判断结果
//        int r = result.intValue();
//        if (r != 0) {
//            // 2.1 如果结果不为0，说明秒杀失败
//            return Result.fail(r == 1 ? "库存不足" : "不允许重复下单");
//        }
//
//        // 2.2 如果结果为0，说明秒杀成功，保存下单信息到阻塞队列
//        VoucherOrder voucherOrder = new VoucherOrder();
//        // 设置订单ID
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        // 设置订单的用户ID
//        userId = UserHolder.getUser().getId();
//        voucherOrder.setUserId(userId);
//        // 设置订单的秒杀券ID
//        voucherOrder.setVoucherId(voucherId);
//        // 放入阻塞队列
//        orderTasks.add(voucherOrder);
//
//        // 3. 获取代理对象（事务）
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//        // 4. 返回订单ID
//        return Result.ok(orderId);
//    }
//

    /**
     * 创建秒杀订单
     * @param voucherOrder 秒杀订单对象
     */
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 1. 一人一单
        long userId = voucherOrder.getUserId();
        // 1.1. 查询用户是否已经购买过该秒杀券
        long count = query().eq("user_id", userId)
                .eq("voucher_id", voucherOrder.getVoucherId())
                .count();
        // 1.2. 如果用户已经购买过，返回失败
        if (count > 0) {
            log.error("用户已经购买过一次");
            return;
        }

        // 2. 扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1") // 原子操作，直接在SQL中更新库存
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0) // 确保库存大于0
                .update();
        if (!success) {
            // 更新失败
            log.error("库存不足");
            return;
        }
        // 3. 创建订单
        save(voucherOrder);
    }


    //    @Override
//    public Result seckillVoucher(Long voucherId) {
//        // 1. 查询秒杀券信息
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        // 2. 判断秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())){
//            return Result.fail("秒杀未开始");
//        }
//        // 3. 判断秒杀是否结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())){
//            return Result.fail("秒杀已结束");
//        }
//        // 4. 判断库存是否充足
//        if (voucher.getStock() <= 0) {
//            return Result.fail("库存不足");
//        }
//        // 5. 利用分布式锁防止超卖
//        Long userId = UserHolder.getUser().getId();
//        // 创建锁对象
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        // 获取锁
//        boolean isLock = lock.tryLock();
//        // 判断是否获取锁成功
//        if (!isLock) {
//            // 获取锁失败，返回错误信息
//            return Result.fail("不允许重复下单");
//        }
//        try {
//            // 获取代理对象（事务）
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            // 释放锁
//            lock.unlock();
//        }
//    }
}
