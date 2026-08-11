configure_file (
    ${CMAKE_CURRENT_LIST_DIR}/AppRun.in
    ${CMAKE_BINARY_DIR}/AppRun
    @ONLY
)

find_program(APPIMAGE NAMES appimagetool)
if (APPIMAGE)
    set (APPIMAGETOOL ${APPIMAGE})
else()
    set (APPIMAGETOOL /usr/local/squashfs-root/AppRun)
endif(APPIMAGE)

configure_file (
    ${CMAKE_CURRENT_LIST_DIR}/build-appimage.sh.in
    ${CMAKE_BINARY_DIR}/build-appimage.sh
    @ONLY
)

add_custom_target(appimage
    COMMAND sh ${CMAKE_BINARY_DIR}/build-appimage.sh
)
