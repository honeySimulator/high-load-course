package ru.quipy.common.utils

import java.util.concurrent.atomic.AtomicInteger

class JobExecutionWindow(
    private val window: NonBlockingOngoingWindow,
) {
    var jobCount: AtomicInteger = AtomicInteger(0)

    fun release() {
        window.releaseWindow()
        jobCount.decrementAndGet()
    }

    fun acquireWindow() {
        window.putIntoWindow()
    }
}