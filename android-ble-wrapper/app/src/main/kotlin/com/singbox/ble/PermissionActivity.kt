package com.singbox.ble

import android.app.Activity
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import android.util.Log

class PermissionActivity : Activity() {

    companion object {
        private const val TAG = "PermissionActivity"
        private const val REQ = 1

        private val BLE_PERMS = buildList {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
                add(android.Manifest.permission.BLUETOOTH_SCAN)
                add(android.Manifest.permission.BLUETOOTH_CONNECT)
            }
            // Required on Android < 12; also covers some Android 12 OEMs with targetSdk ≤ 31
            add(android.Manifest.permission.ACCESS_FINE_LOCATION)
        }.toTypedArray()
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        if (BLE_PERMS.isEmpty() || BLE_PERMS.all {
                checkSelfPermission(it) == PackageManager.PERMISSION_GRANTED
            }) {
            startServiceAndFinish()
        } else {
            requestPermissions(BLE_PERMS, REQ)
        }
    }

    override fun onRequestPermissionsResult(
        requestCode: Int, permissions: Array<out String>, grantResults: IntArray
    ) {
        if (requestCode == REQ) {
            val denied = grantResults.indices.filter {
                grantResults[it] != PackageManager.PERMISSION_GRANTED
            }.map { permissions[it] }
            if (denied.isEmpty()) {
                startServiceAndFinish()
            } else {
                Log.e(TAG, "permissions denied: $denied")
                finish()
            }
        }
    }

    private fun startServiceAndFinish() {
        val svc = Intent(this, BleService::class.java)
        intent.extras?.let { svc.putExtras(it) }
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            startForegroundService(svc)
        } else {
            startService(svc)
        }
        finish()
    }
}
