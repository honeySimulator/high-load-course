package ru.quipy.common.utils

import com.google.common.collect.EvictingQueue
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class MyCircuitBreaker(
    val name: String,
    private val failureRateThreshold: Double,
    private val slidingWindowSize: Int,
    private val resetTimeoutMs: Long
) {

    enum class CircuitBreakerState {
        CLOSED, OPEN, HALF_OPEN
    }

    private var lastStateChangeTime = System.currentTimeMillis()
    private var state = CircuitBreakerState.CLOSED
    private val executor = Executors.newSingleThreadScheduledExecutor(NamedThreadFactory(name))
    private val logger = LoggerFactory.getLogger(MyCircuitBreaker::class.java)
    private val window = EvictingQueue.create<Boolean>(slidingWindowSize)

    init {
        executor.scheduleAtFixedRate({
            update()
        }, 0, 5_000, TimeUnit.MILLISECONDS)
    }

    fun canMakeCall() = state != CircuitBreakerState.OPEN

    fun submitExecution() {
        if (state == CircuitBreakerState.OPEN) throw CircuitBreakerOpenException("Circuit breaker is open")
    }

    fun submitFailure() {
        incrementFailureCount()
        when (state) {
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
        when (state) {
            CircuitBreakerState.CLOSED -> {}
            CircuitBreakerState.OPEN -> {}
            CircuitBreakerState.HALF_OPEN -> {
                resetFailureCount()
                transitionToClosed()
            }
        }
    }

    private fun incrementCallCount() {
        window.add(true)
    }

    private fun incrementFailureCount() {
        window.add(false)
    }

    private fun resetFailureCount() {
        window.clear()
    }

    private fun failureRate(): Double {
        if (window.isEmpty()) {
            return 0.0
        }
        val calls = window.toArray()
        return calls.count { x -> x == true }.toDouble() / calls.size.toDouble()
    }

    private fun transitionToOpen() {
        if (state != CircuitBreakerState.OPEN) {
            state = CircuitBreakerState.OPEN
            lastStateChangeTime = System.currentTimeMillis()
            onStateChange(state)
        }
    }

    private fun transitionToHalfOpen() {
        if (state != CircuitBreakerState.HALF_OPEN) {
            state = CircuitBreakerState.HALF_OPEN
            lastStateChangeTime = System.currentTimeMillis()
            onStateChange(state)
        }
    }

    private fun transitionToClosed() {
        if (state != CircuitBreakerState.CLOSED) {
            state = CircuitBreakerState.CLOSED
            lastStateChangeTime = System.currentTimeMillis()
            onStateChange(state)
        }
    }

    private fun onStateChange(state: CircuitBreakerState) {
        logger.error("[$name] now in state $state")
    }

    fun update() {
        if (state == CircuitBreakerState.OPEN && System.currentTimeMillis() - lastStateChangeTime >= resetTimeoutMs) {
            transitionToHalfOpen()
        }
    }
}

class CircuitBreakerOpenException(message: String) : RuntimeException(message)