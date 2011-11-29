# - Find Mapr
# Find the native Mapr includes and library
#
#  Mapr_INCLUDE_DIR - where to find Mapr.h, etc.
#  Mapr_LIBRARIES   - List of libraries when using Mapr.
#  Mapr_FOUND       - True if Mapr found.


if (Mapr_INCLUDE_DIR)
  # Already in cache, be silent
  set(Mapr_FIND_QUIETLY TRUE)
endif ()

find_path(Mapr_INCLUDE_DIR hdfs.h
  $ENV{HADOOP_HOME}/src/c++/libhdfs
  /usr/lib/hadoop/src/c++/libhdfs
  /opt/local/include
  /usr/local/include
)

# NOTE:  Ditch this once ported to MapR
find_path(Jni_INCLUDE_DIR jni.h
  /System/Library/Frameworks/JavaVM.framework/Headers
)
mark_as_advanced(Jni_INCLUDE_DIR)

macro(FIND_MAPR_LIB lib)
  find_library(${lib}_LIB NAMES ${lib}
    PATHS $ENV{HADOOP_HOME}/src/c++/install/lib/
        /usr/lib/hadoop/src/c++/install/lib/
        $ENV{HADOOP_HOME}/c++/lib /usr/lib/hadoop/c++/lib
        /opt/local/lib /usr/local/lib
  )
  mark_as_advanced(${lib}_LIB)
endmacro(FIND_MAPR_LIB lib libname)

FIND_MAPR_LIB(hdfs)

if (Mapr_INCLUDE_DIR)
  set(Mapr_FOUND TRUE)
  set(Mapr_LIBRARIES ${hdfs_LIB})
else ()
   set(Mapr_FOUND FALSE)
   set(Mapr_LIBRARIES)
endif ()

if (Mapr_FOUND)
   if (NOT Mapr_FIND_QUIETLY)
      message(STATUS "Found MAPR: ${Mapr_LIBRARIES}")
   endif ()
else ()
   if (Mapr_FIND_REQUIRED)
      message(FATAL_ERROR "Could NOT find MAPR libraries")
   endif ()
endif ()

mark_as_advanced(
  Mapr_INCLUDE_DIR
)
