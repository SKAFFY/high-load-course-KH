package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.math.pow
import kotlin.time.toDurationUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private var rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong() - 1, Duration.ofSeconds(1))
    private var ongoingWindow = OngoingWindow(parallelRequests)
    // 8 пар запр каждый 5 8/5 = 1.(....)

    //private val client = OkHttpClient.Builder().build()
    private val client = OkHttpClient.Builder()
        .callTimeout(Duration.ofMillis(2000L))
        .build()

    private val boundedQueue = LinkedBlockingQueue<Runnable>(10)
    private val threadPool = ThreadPoolExecutor(
        8,
        16, // maximumPoolSize
        15,
        TimeUnit.MINUTES,
        boundedQueue, // priority workQueue**
        Executors.defaultThreadFactory(),
        ThreadPoolExecutor.AbortPolicy()
    )


//    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
//        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
//
//        val transactionId = UUID.randomUUID()
//        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")
//
//        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
//        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
//        paymentESService.update(paymentId) {
//            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
//        }
//
//        if (deadline - now() < 0) {
//            logger.warn("[$accountName] Payment deadline reached for txId: $transactionId, payment: $paymentId" + "now: ${now()}, deadline: $deadline, payment started at $paymentStartedAt")
//            paymentESService.update(paymentId) {
//                it.logProcessing(false, now(), transactionId, reason = "Deadline reached.")
//            }
//
//            return
//        }
//
//        ongoingWindow.acquire()
//        rateLimiter.tickBlocking()
//
//
//        val request = Request.Builder().run {
//            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
//            post(emptyBody)
//        }.build()
//
//        try {
//            client.newCall(request).execute().use { response ->
//                val body = try {
//                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
//                } catch (e: Exception) {
//                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
//                    ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
//                }
//
//                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
//
//                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
//                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
//                paymentESService.update(paymentId) {
//                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
//                }
//            }
//        } catch (e: Exception) {
//            when (e) {
//                is SocketTimeoutException -> {
//                    logger.warn("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
//                    paymentESService.update(paymentId) {
//                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
//                    }
//                }
//
//                else -> {
//                    logger.warn("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
//
//                    paymentESService.update(paymentId) {
//                        it.logProcessing(false, now(), transactionId, reason = e.message)
//                    }
//                }
//            }
//        } finally {
//            ongoingWindow.release()
//        }
//    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val future = threadPool.execute(ImportantTask(amount.toLong()) {
            performPayment(paymentId, amount, paymentStartedAt, deadline)
        })
    }


    private fun performPayment(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId, txId: $transactionId")

        if (deadline - now() < 0) {
            logger.error("[$accountName] Payment deadline reached for txId: $transactionId, payment: $paymentId")

            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "deadline reached")
            }

            return
        }

        ongoingWindow.acquire()

        try {
            // Логируем отправку платежа
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            rateLimiter.tickBlocking()

            val request = Request.Builder()
                .url("http://localhost:1234/external/process?serviceName=$serviceName&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                .post(emptyBody)
                .build()

            var responseBody: ExternalSysResponse? = null
            var attempt = 0
            var success = false

            while (attempt < 3 && !success) {
                client.newCall(request).execute().use { response ->
                    val responseStr = response.body?.string()

                    responseBody = try {
                        mapper.readValue(responseStr, ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] Ошибка при разборе ответа, код: ${response.code}, тело: $responseStr")
                        ExternalSysResponse(
                            transactionId.toString(),
                            paymentId.toString(),
                            false,
                            "Ошибка парсинга JSON"
                        )
                    }

                    success = responseBody!!.result

                    // Если неуспешно и код позволяет ретрай, делаем экспоненциальную задержку
                    if (!success && response.code in listOf(429, 500, 502, 503, 504)) {
                        attempt++
                        if (attempt < 3) {
                            val delay = (100L * 2.0.pow(attempt)).toLong()
                            logger.warn("[$accountName] Повторная попытка $attempt для $paymentId (код: ${response.code}), задержка: $delay мс")
                            Thread.sleep(delay)
                        }
                    }
                }
            }

            // Логируем результат
            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${responseBody?.result}, message: ${responseBody?.message}")

            paymentESService.update(paymentId) {
                it.logProcessing(responseBody?.result ?: false, now(), transactionId, reason = responseBody?.message)
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.warn("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.warn("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        } finally {
            ongoingWindow.release()
        }
    }


    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()

data class ImportantTask(
    val levelOfImportency: Long,
    val block: (ImportantTask) -> Unit
) : Runnable, Comparable<ImportantTask> {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
    }

    override fun run() {
        try {
            block(this)
        } catch (e: Exception) {
            logger.error("Important task failed", e)
        }
    }

    override fun compareTo(other: ImportantTask): Int {
        return levelOfImportency.compareTo(other.levelOfImportency)
    }
}