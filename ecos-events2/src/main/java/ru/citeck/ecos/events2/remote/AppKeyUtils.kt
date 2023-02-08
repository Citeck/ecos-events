package ru.citeck.ecos.events2.remote

import ru.citeck.ecos.commons.utils.NameUtils

object AppKeyUtils {

    private const val TRANSACTIONAL_KEY_POSTFIX = ".T11L"

    private val TARGET_APP_NAME_PART_ESCAPER = NameUtils.getEscaperWithAllowedChars("-:")

    fun getAppName(key: String): String {
        return TARGET_APP_NAME_PART_ESCAPER.unescape(key.substringBefore("."))
    }

    fun createKey(appName: String, appInstanceId: String, exclusive: Boolean, transactional: Boolean = false): String {
        if (!exclusive && transactional) {
            error("Event can't be transactional and not exclusive. App: $appName InstanceId: $appInstanceId")
        }
        var key = TARGET_APP_NAME_PART_ESCAPER.escape(appName)
        if (!exclusive) {
            key += "." + TARGET_APP_NAME_PART_ESCAPER.escape(appInstanceId)
        } else if (transactional) {
            key += TRANSACTIONAL_KEY_POSTFIX
        }
        return key
    }

    fun isKeyExclusive(key: String): Boolean {
        return !key.contains('.') || isKeyTransactional(key)
    }

    fun isKeyTransactional(key: String): Boolean {
        return key.endsWith(TRANSACTIONAL_KEY_POSTFIX)
    }

    fun isKeyForApp(appName: String, appInstanceId: String, targetKey: String): Boolean {
        return createKey(
            appName,
            appInstanceId,
            isKeyExclusive(targetKey),
            isKeyTransactional(targetKey)
        ) == targetKey
    }
}
