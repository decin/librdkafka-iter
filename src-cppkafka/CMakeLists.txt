set(LIBVER 1)

# 添加 boost 头文件
include_directories(/usr/local/include)

aux_source_directory(. SRC_SOURCE)
set(
    sources ${SRC_SOURCE}
)

if(RDKAFKA_BUILD_STATIC)
  set(CMAKE_POSITION_INDEPENDENT_CODE ON)
  set(RDKAFKA_BUILD_MODE STATIC)
else()
  set(RDKAFKA_BUILD_MODE SHARED)
endif()

add_library(sqkafka ${RDKAFKA_BUILD_MODE} ${sources})
if(NOT RDKAFKA_BUILD_STATIC)
  set_property(TARGET sqkafka PROPERTY SOVERSION ${LIBVER})
endif()

target_link_libraries(sqkafka PUBLIC rdkafka)

# Support '#include <rdkafcpp.h>'
target_include_directories(sqkafka PUBLIC "$<BUILD_INTERFACE:${CMAKE_CURRENT_LIST_DIR}>")
if(NOT RDKAFKA_BUILD_STATIC)
    target_compile_definitions(sqkafka PRIVATE LIBRDKAFKACPP_EXPORTS)
endif()

# Generate pkg-config file
set(PKG_CONFIG_VERSION "${PROJECT_VERSION}")
if(NOT RDKAFKA_BUILD_STATIC)
  set(PKG_CONFIG_NAME "libsqkafka")
  set(PKG_CONFIG_DESCRIPTION "The Apache Kafka C/C++ library")
  set(PKG_CONFIG_REQUIRES_PRIVATE "rdkafka")
  set(PKG_CONFIG_CFLAGS "-I\${includedir}")
  set(PKG_CONFIG_LIBS "-L\${libdir} -lsqkafka")
  set(PKG_CONFIG_LIBS_PRIVATE "-lrdkafka")
  configure_file(
    "../packaging/cmake/rdkafka.pc.in"
    "${GENERATED_DIR}/sqkafka.pc"
    @ONLY
  )
  install(
    FILES ${GENERATED_DIR}/sqkafka.pc
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig"
  )
else()
  set(PKG_CONFIG_NAME "libsqkafka-static")
  set(PKG_CONFIG_DESCRIPTION "The Apache Kafka C/C++ library (static)")
  set(PKG_CONFIG_REQUIRES_PRIVATE "")
  set(PKG_CONFIG_CFLAGS "-I\${includedir} -DLIBRDKAFKA_STATICLIB")
  set(PKG_CONFIG_LIBS "-L\${libdir} \${libdir}/libsqkafka.a")
  if(WIN32)
    string(APPEND PKG_CONFIG_LIBS " -lws2_32 -lsecur32 -lcrypt32")
  endif()

  configure_file(
    "../packaging/cmake/rdkafka.pc.in"
    "${GENERATED_DIR}/sqkafka-static.pc"
    @ONLY
  )
  install(
    FILES ${GENERATED_DIR}/sqkafka-static.pc
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig"
  )
endif()

# target_link_libraries( sqkafka log )

install(
    TARGETS sqkafka
    EXPORT "${targets_export_name}"
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
    INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)

install(
    FILES "rdkafkacpp.h"
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/librdkafka"
)
