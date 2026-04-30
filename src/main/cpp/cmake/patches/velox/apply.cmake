# Applies Velox patches in order.
# Invoked by FetchContent_Declare's PATCH_COMMAND.
# Variables: SOURCE_DIR, PATCH_DIR

set(PATCHES)

foreach(patch IN LISTS PATCHES)
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
