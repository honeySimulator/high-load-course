package ru.quipy.common.utils

import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicIntegerArray
import java.util.concurrent.atomic.AtomicReference

class MyCircuitBreaker(
    val name: String,
    private val failureRateThreshold: Double,
    private val slidingWindowSize: Int,
    private val resetTimeoutMs: Long
) {

    enum class CircuitBreakerState {
        CLOSED, OPEN, HALF_OPEN
    }

    private var lastStateChangeTime = AtomicReference(System.currentTimeMillis())
    private var state = AtomicReference(CircuitBreakerState.CLOSED)
    private val executor = Executors.newSingleThreadScheduledExecutor(NamedThreadFactory(name))
    private val logger = LoggerFactory.getLogger(MyCircuitBreaker::class.java)
    private val window = AtomicIntegerArray(slidingWindowSize)

    init {
        executor.scheduleAtFixedRate({
            update()
        }, 0, 5_000, TimeUnit.MILLISECONDS)
    }

    fun canMakeCall() = state.get() != CircuitBreakerState.OPEN

    fun submitExecution() {
        if (state.get() == CircuitBreakerState.OPEN) throw CircuitBreakerOpenException("Circuit breaker is open")
    }

    fun submitFailure() {
        incrementFailureCount()
        when (state.get()) {
            CircuitBreakerState.CLOSED -> {
                if (failureRate() >= failureRateThreshold) {
                    transitionToOpen()
                }
            }
            CircuitBreakerState.OPEN -> {}
            CircuitBreakerState.HALF_OPEN -> {
                transitionToOpen()
            }
        }
    }

    fun submitSuccess() {
        incrementCallCount()
        when (state.get()) {
            CircuitBreakerState.CLOSED -> {}
            CircuitBreakerState.OPEN -> {}
            CircuitBreakerState.HALF_OPEN -> {
                resetFailureCount()
                transitionToClosed()
            }
        }
    }

    private fun incrementCallCount() {
        window.incrementAndGet(window.length() - 1)
    }

    private fun incrementFailureCount() {
        window.decrementAndGet(window.length() - 1)
    }

    private fun resetFailureCount() {
        for (i in 0 until window.length()) {
            window.set(i, 0)
        }
    }

    private fun sum(): Int {
        var sum = 0
        for (i in 0 until window.length()) {
            sum += window.get(i)
        }
        return sum
    }

    private fun failureRate(): Double {
        val total = sum()
        if (total == 0) return 0.0
        val failures = total - window.get(window.length() - 1)
        return failures.toDouble() / total.toDouble()
    }

    private fun transitionToOpen() {
        state.compareAndSet(CircuitBreakerState.CLOSED, CircuitBreakerState.OPEN)
        lastStateChangeTime.set(System.currentTimeMillis())
        onStateChange(state.get())
    }

    private fun transitionToHalfOpen() {
        state.compareAndSet(CircuitBreakerState.OPEN, CircuitBreakerState.HALF_OPEN)
        lastStateChangeTime.set(System.currentTimeMillis())
        onStateChange(state.get())
    }

    private fun transitionToClosed() {
        state.compareAndSet(CircuitBreakerState.HALF_OPEN, CircuitBreakerState.CLOSED)
        lastStateChangeTime.set(System.currentTimeMillis())
        onStateChange(state.get())
    }

    private fun onStateChange(state: CircuitBreakerState) {
        logger.error("[$name] now in state $state")
    }

    fun update() {
        if (state.get() == CircuitBreakerState.OPEN && System.currentTimeMillis() - lastStateChangeTime.get() >= resetTimeoutMs) {
            transitionToHalfOpen()
        }
    }

    fun destroy() {
        logger.info("Shutting down the CircuitBreaker executor")
        executor.shutdown()
//        try {
//            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
//                executor.shutdownNow()
//            }
//        } catch (e: InterruptedException) {
//            executor.shutdownNow()
//            Thread.currentThread().interrupt()
//        }
    }
}

class CircuitBreakerOpenException(message: String) : RuntimeException(message)