execute_process(
    COMMAND sed -n "s/^.*(\\(.*\\)).*$/\\1/p;q" ${CMAKE_HOME_DIRECTORY}/ChangeLog
    OUTPUT_VARIABLE MANDATE
)

set (MANPAGES "")
foreach (FILE dbt5.1
              dbt5-build.1
              dbt5-build-egen.1
              dbt5-post-process.1
              dbt5-run.1
        )
    configure_file(${FILE}.rst.in ${CMAKE_BINARY_DIR}/man/man1/${FILE}.rst)
    execute_process(
            COMMAND rst2man ${CMAKE_BINARY_DIR}/man/man1/${FILE}.rst
            OUTPUT_FILE ${CMAKE_BINARY_DIR}/man/man1/${FILE}
    )
    set (MANPAGES ${MANPAGES} ${CMAKE_BINARY_DIR}/man/man1/${FILE})
endforeach()

install(FILES ${MANPAGES} DESTINATION "share/man/man1")
