# Keep ZappMessaging SDK public API
-keep class xyz.justzappit.zappmessaging.ZappMessagingSDK { *; }
-keep class xyz.justzappit.zappmessaging.models.** { *; }

# Keep kotlinx.serialization
-keepattributes *Annotation*, InnerClasses
-dontnote kotlinx.serialization.AnnotationsKt
-keepclassmembers class kotlinx.serialization.json.** { *** Companion; }
-keepclasseswithmembers class kotlinx.serialization.json.** {
    kotlinx.serialization.KSerializer serializer(...);
}
-keep,includedescriptorclasses class xyz.justzappit.zappmessaging.models.**$$serializer { *; }
-keepclassmembers class xyz.justzappit.zappmessaging.models.** {
    *** Companion;
}
-keepclasseswithmembers class xyz.justzappit.zappmessaging.models.** {
    kotlinx.serialization.KSerializer serializer(...);
}
