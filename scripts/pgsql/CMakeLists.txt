set (PROGRAMFILES "")

foreach (FILE dbt5-pgsql-build-db
              dbt5-pgsql-check-db
              dbt5-pgsql-create-db
              dbt5-pgsql-create-indexes
              dbt5-pgsql-create-tables
              dbt5-pgsql-drop-stored-procs
              dbt5-pgsql-load-stored-procs
              dbt5-pgsql-plans
              dbt5-pgsql-set-param
        )
    configure_file(${FILE}.in ${CMAKE_BINARY_DIR}/${FILE} @ONLY)
    set (PROGRAMFILES ${PROGRAMFILES} ${CMAKE_BINARY_DIR}/${FILE})
endforeach()

install (PROGRAMS ${PROGRAMFILES} DESTINATION "bin")
