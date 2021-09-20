package ru.citeck.ecos.events2.remote

import ru.citeck.ecos.commons.utils.NameUtils

object AppKeyUtils {

    private val TARGET_APP_NAME_PART_ESCAPER = NameUtils.getEscaperWithAllowedChars("-:")

    fun createKey(appName: String, appInstanceId: String, exclusive: Boolean): String {
        val exclusiveName = TARGET_APP_NAME_PART_ESCAPER.escape(appName)
        if (exclusive) {
            return exclusiveName
        }
        return exclusiveName + "." + TARGET_APP_NAME_PART_ESCAPER.escape(appInstanceId)
    }

    fun isKeyExclusive(key: String): Boolean {
        return !key.contains('.')
    }

    fun isKeyForApp(appName: String, appInstanceId: String, targetKey: String): Boolean {
        return createKey(appName, appInstanceId, isKeyExclusive(targetKey)) == targetKey
    }
}