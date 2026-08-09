import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import java.util.Properties

plugins {
    id("com.android.library")
    id("org.jetbrains.kotlin.android")
    id("org.jetbrains.kotlin.plugin.serialization")
}

// Resolve blind-peer public keys (comma-separated z32 / hex) at configuration time.
// Lookup order: rootProject local.properties → -P / gradle.properties → BLIND_PEER_KEYS env → "".
// Empty value disables blind mirroring at runtime (see BareWorkletManager + core/lib/config.js).
val localProps: Properties = Properties().apply {
    val f = rootProject.file("local.properties")
    if (f.exists()) f.inputStream().use { load(it) }
}

val blindPeerKeys: String = localProps.getProperty("BLIND_PEER_KEYS")
    ?: providers.gradleProperty("BLIND_PEER_KEYS").orNull
    ?: providers.environmentVariable("BLIND_PEER_KEYS").orNull
    ?: ""

// Optional custom DHT bootstrap nodes (comma-separated host:port). When set,
// these are appended to the default Holepunch bootstrap list. Adding the
// blind-peer's own host here guarantees the phone's initial DHT routing table
// includes a node that knows about the blind-peer's announce — solves the
// "PEER_NOT_FOUND from fresh DHT" path when the blind-peer is on a public VPS.
val blindPeerBootstrap: String = localProps.getProperty("BLIND_PEER_BOOTSTRAP")
    ?: providers.gradleProperty("BLIND_PEER_BOOTSTRAP").orNull
    ?: providers.environmentVariable("BLIND_PEER_BOOTSTRAP").orNull
    ?: ""

// Optional stable public address of the blind peer itself. HyperDHT tries it
// immediately while retaining normal DHT discovery as a fallback.
val blindPeerAddress: String = localProps.getProperty("BLIND_PEER_ADDRESS")
    ?: providers.gradleProperty("BLIND_PEER_ADDRESS").orNull
    ?: providers.environmentVariable("BLIND_PEER_ADDRESS").orNull
    ?: ""

// Authenticated HTTPS fallback for the encrypted bootstrap invite mailbox.
// This is used when carrier/router policy prevents HyperDHT's UDP transport
// from reaching the blind peer even at its fixed public address.
val inviteMailboxUrl: String = localProps.getProperty("INVITE_MAILBOX_URL")
    ?: providers.gradleProperty("INVITE_MAILBOX_URL").orNull
    ?: providers.environmentVariable("INVITE_MAILBOX_URL").orNull
    ?: ""

// Worklet log level: 'debug' | 'info' | 'warn' | 'error' | 'off'. Empty = use
// JS-side default ('info'). Set ZAPP_MESSAGING_LOG_LEVEL=debug to enable verbose
// stream/probe/keypair diagnostics in blind-mirror & p2p-manager.
val zappMessagingLogLevel: String = localProps.getProperty("ZAPP_MESSAGING_LOG_LEVEL")
    ?: providers.gradleProperty("ZAPP_MESSAGING_LOG_LEVEL").orNull
    ?: providers.environmentVariable("ZAPP_MESSAGING_LOG_LEVEL").orNull
    ?: ""

android {
    namespace = "xyz.justzappit.zappmessaging"
    compileSdk = 36

    defaultConfig {
        minSdk = 29
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
        consumerProguardFiles("consumer-rules.pro")
        buildConfigField("String", "BLIND_PEER_KEYS", "\"${blindPeerKeys.replace("\"", "\\\"")}\"")
        buildConfigField("String", "BLIND_PEER_BOOTSTRAP", "\"${blindPeerBootstrap.replace("\"", "\\\"")}\"")
        buildConfigField("String", "BLIND_PEER_ADDRESS", "\"${blindPeerAddress.replace("\"", "\\\"")}\"")
        buildConfigField("String", "INVITE_MAILBOX_URL", "\"${inviteMailboxUrl.replace("\"", "\\\"")}\"")
        buildConfigField("String", "ZAPP_MESSAGING_LOG_LEVEL", "\"${zappMessagingLogLevel.replace("\"", "\\\"")}\"")
    }

    buildFeatures {
        buildConfig = true
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro"
            )
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

}

kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
    }
}

dependencies {
    // BareKit Android SDK — subproject from bare-kit/android
    api(project(":bare-kit"))

    // Kotlin coroutines
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.7.3")

    // JSON serialization
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.2")

    // Core AndroidX
    implementation("androidx.core:core-ktx:1.12.0")

    // Testing
    testImplementation("junit:junit:4.13.2")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.7.3")
}
