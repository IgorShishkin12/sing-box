package com.singbox.ble

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.bluetooth.BluetoothManager
import android.content.Intent
import android.content.pm.PackageManager
import android.content.pm.ServiceInfo
import android.os.Build
import android.os.IBinder
import android.util.Log

class BleService : Service() {

    companion object {
        const val TAG = "BleService"
        const val EXTRA_CONFIG_JSON = "config_json"
        const val EXTRA_CONFIG_PATH = "config_path"
        private const val NOTIF_CHANNEL = "ble_service"
        private const val NOTIF_ID = 1
    }

    override fun onCreate() {
        super.onCreate()
        Bridge.nativeSetJVM()
        Log.i(TAG, "bridge JVM registered")
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        startForegroundCompat()

        val config = when {
            intent?.hasExtra(EXTRA_CONFIG_JSON) == true ->
                intent.getStringExtra(EXTRA_CONFIG_JSON) ?: "{}"
            intent?.hasExtra(EXTRA_CONFIG_PATH) == true ->
                java.io.File(intent.getStringExtra(EXTRA_CONFIG_PATH)!!).readText()
            else -> "{}"
        }

        val storagePath = filesDir.absolutePath + "/reticulum"
        java.io.File(storagePath).mkdirs()

        // Diagnose BT state before handing off to native code
        val btMgr = getSystemService(android.bluetooth.BluetoothManager::class.java)
        val btAdapter = btMgr?.adapter
        Log.i(TAG, "BT adapter=${btAdapter != null} enabled=${btAdapter?.isEnabled} " +
            "scanner=${btAdapter?.bluetoothLeScanner != null}")

        if (Bridge.nativeInit(config, storagePath) != 0) {
            Log.e(TAG, "bridge init failed")
            stopSelf()
        } else {
            Log.i(TAG, "bridge started")
        }
        return START_NOT_STICKY
    }

    override fun onDestroy() {
        Bridge.nativeShutdown()
        Log.i(TAG, "bridge stopped")
        super.onDestroy()
    }

    override fun onBind(intent: Intent?): IBinder? = null

    private fun startForegroundCompat() {
        val mgr = getSystemService(NotificationManager::class.java)
        mgr.createNotificationChannel(
            NotificationChannel(NOTIF_CHANNEL, "BLE Bridge", NotificationManager.IMPORTANCE_LOW)
        )
        val notif = Notification.Builder(this, NOTIF_CHANNEL)
            .setContentTitle("SingBox BLE")
            .setContentText("Reticulum BLE bridge running")
            .setSmallIcon(android.R.drawable.stat_sys_data_bluetooth)
            .build()

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
            startForeground(NOTIF_ID, notif,
                ServiceInfo.FOREGROUND_SERVICE_TYPE_CONNECTED_DEVICE)
        } else {
            startForeground(NOTIF_ID, notif)
        }
    }

}
