# Applies Velox patches in order.
# Invoked by FetchContent_Declare's PATCH_COMMAND.
# Variables: SOURCE_DIR, PATCH_DIR
foreach(patch IN ITEMS "iconfig-gcc-bug-103186.patch"
                       "fix-s2geometry-absl-dep.patch")
  execute_process(
    COMMAND patch -p1 -i "${PATCH_DIR}/${patch}"
    WORKING_DIRECTORY "${SOURCE_DIR}"
    RESULT_VARIABLE result)
  if(NOT result EQUAL 0)
    message(
      FATAL_ERROR
        "Failed to apply Velox patch '${patch}' (exit code: ${result})")
  endif()
endforeach()
