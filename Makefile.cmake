# vim: set ft=make :

.PHONY: appimage clean debug package release

default:
	@echo "targets: appimage (Linux only), clean, debug, package, release"

appimage:
	cmake -H. -Bbuilds/appimage -DCMAKE_INSTALL_PREFIX=/usr
	cd builds/appimage && make install DESTDIR=../AppDir
	@if [ ! "$(EGEN)" = "" ]; then \
		mkdir -p builds/AppDir/opt/egen && \
		unzip -d builds/AppDir/opt/egen "$(EGEN)" && \
		builds/AppDir/usr/bin/dbt5-build-egen --include-dir=src/include \
				--patch-dir=patches --source-dir=src \
				builds/AppDir/opt/egen; \
	fi
	cd builds/appimage && make appimage

clean:
	-rm -rf builds

debug:
	cmake -H. -Bbuilds/debug -DCMAKE_BUILD_TYPE=Debug
	cd builds/debug && make

package:
	git checkout-index --prefix=builds/source/ -a
	cmake -Hbuilds/source -Bbuilds/source
	cd builds/source && make package_source

release:
	cmake -H. -Bbuilds/release
	cd builds/release && make
