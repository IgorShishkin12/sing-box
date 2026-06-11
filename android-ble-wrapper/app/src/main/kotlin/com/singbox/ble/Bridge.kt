package com.singbox.ble

object Bridge {
    init { System.loadLibrary("sing-box-ble") }

    external fun nativeSetJVM()
    external fun nativeInit(configJson: String): Int
    external fun nativeShutdown()
}
