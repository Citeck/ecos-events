package ru.citeck.ecos.events2.rabbitmq

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.events2.remote.AppKeyUtils

class AppKeyUtilsTest {

    @Test
    fun keyUtilsTest() {

        val appName = "appName"
        val appInstanceId = "appInstanceId"

        val exclusiveKey = AppKeyUtils.createKey(appName, appInstanceId, true)
        assertThat(exclusiveKey).isEqualTo(appName)
        assertThat(AppKeyUtils.isKeyForApp(appName, appInstanceId, exclusiveKey)).isTrue
        assertThat(AppKeyUtils.isKeyExclusive(exclusiveKey)).isTrue

        val inclusiveKey = AppKeyUtils.createKey(appName, appInstanceId, false)
        assertThat(inclusiveKey).isEqualTo("$appName.$appInstanceId")
        assertThat(AppKeyUtils.isKeyForApp(appName, appInstanceId, inclusiveKey)).isTrue
        assertThat(AppKeyUtils.isKeyExclusive(inclusiveKey)).isFalse

        val appNameWithDot = "instance.id"
        val escapedAppNameWithDot = "instance_u002E_id"

        val exclusiveKeyWithDot = AppKeyUtils.createKey(appNameWithDot, appInstanceId, true)
        assertThat(exclusiveKeyWithDot).isEqualTo(escapedAppNameWithDot)
        assertThat(AppKeyUtils.isKeyForApp(appNameWithDot, appInstanceId, exclusiveKeyWithDot)).isTrue
        assertThat(AppKeyUtils.isKeyExclusive(exclusiveKeyWithDot)).isTrue

        val inclusiveKeyWithDot = AppKeyUtils.createKey(appNameWithDot, appInstanceId, false)
        assertThat(inclusiveKeyWithDot).isEqualTo("$escapedAppNameWithDot.$appInstanceId")
        assertThat(AppKeyUtils.isKeyForApp(appNameWithDot, appInstanceId, inclusiveKeyWithDot)).isTrue
        assertThat(AppKeyUtils.isKeyExclusive(inclusiveKeyWithDot)).isFalse
    }
}