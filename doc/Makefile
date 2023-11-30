.PHONY: default clean html pdf

RST=$(wildcard *.rst)

default:
	@echo "targets: html, pdf"

clean:
	-rm -rf html pdf

html: $(RST)
	mkdir -p html
	rst2html5 dbt5.rst html/index.html

pdf: html
	mkdir -p pdf
	pandoc -s html/index.html -t pdf -o pdf/dbt5.pdf
