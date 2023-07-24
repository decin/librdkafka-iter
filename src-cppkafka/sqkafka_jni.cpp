#include <jni.h>
#include <string>
#include <iostream>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>
#include "cppkafka.h"

using namespace std;
using namespace cppkafka;
#define mNativeKafka "mNativeKafka"

extern "C"
JNIEXPORT jlong JNICALL
Java_co_timekettle_kafka_KafkaClient_KafkaOpen(JNIEnv *env, jobject thiz, jstring jBrokers, jint timeout) {
    const char *brokersChars = env->GetStringUTFChars(jBrokers, nullptr);
    std::string brokers(brokersChars);

    // Create the config
    Configuration config = {
            { "metadata.broker.list", brokers },
//            { "api.version.request", "false" },
    };
    // Create the producer
    auto *producer = new Producer(config);
    if (timeout > 1000) producer->set_timeout(std::chrono::milliseconds(timeout));

    auto ptr = reinterpret_cast<jlong>(producer);
    jclass clazz = env->GetObjectClass(thiz);
    jfieldID fieldID = env->GetFieldID(clazz, mNativeKafka, "J");
    env->SetLongField(thiz, fieldID, ptr);

    env->ReleaseStringUTFChars(jBrokers, brokersChars);
    return (jlong)producer;
}

extern "C"
JNIEXPORT jstring JNICALL
Java_co_timekettle_kafka_KafkaClient_KafkaSend(JNIEnv *env, jobject thiz, jstring jTopic,
                                               jstring jMsg) {
    jclass clazz = env->GetObjectClass(thiz);
    jfieldID fieldID = env->GetFieldID(clazz, mNativeKafka, "J");
    jlong ptr = env->GetLongField(thiz, fieldID);
    auto producer = reinterpret_cast<Producer *>(ptr);

    const char *topicChars = env->GetStringUTFChars(jTopic, nullptr);
    std::string topic(topicChars);
    const char *msgChars = env->GetStringUTFChars(jMsg, nullptr);
    std::string msg(msgChars);

    const char *err = nullptr;
    try {
        producer->produce(MessageBuilder(topicChars).payload(msg));
//        producer->flush();
    } catch (Exception e) {
        err = e.what();
    }

    env->ReleaseStringUTFChars(jTopic, topicChars);
    env->ReleaseStringUTFChars(jMsg, msgChars);
    return err != nullptr ? env->NewStringUTF(err) : nullptr;
}

extern "C"
JNIEXPORT void JNICALL
Java_co_timekettle_kafka_KafkaClient_KafkaClose(JNIEnv *env, jobject thiz) {
    jclass clazz = env->GetObjectClass(thiz);
    jfieldID fieldID = env->GetFieldID(clazz, mNativeKafka, "J");
    jlong ptr = env->GetLongField(thiz, fieldID);
    auto producer = reinterpret_cast<Producer *>(ptr);
    delete producer;
}