.PHONY: appimage clean debug egen package release

default:
	@echo "targets: appimage (Linux only), clean, debug, package, release"

appimage:
	cmake -H. -Bbuilds/appimage -DCMAKE_INSTALL_PREFIX=/usr
	cd builds/appimage && make -s install DESTDIR=AppDir
	cd builds/appimage && make -s appimage-podman

UNAME_S := $(shell uname -s)

clean:
	-rm -rf builds

debug:
	cmake -H. -Bbuilds/debug -DCMAKE_BUILD_TYPE=Debug
	cd builds/debug && make

egen:
	cmake -H. -Bbuilds/appimage -DCMAKE_INSTALL_PREFIX=/usr
	cd builds/appimage && make -s install DESTDIR=AppDir
	mkdir -p /usr/local/AppDir/opt/
	cp -pr egen /usr/local/AppDir/opt/
	builds/appimage/AppDir/usr/bin/dbt5-build-egen --include-dir=src/include \
			--patch-dir=patches --source-dir=src \
			/usr/local/AppDir/opt/egen
	cd builds/appimage && make -s appimage-podman

package:
	git checkout-index --prefix=builds/source/ -a
	cmake -Hbuilds/source -Bbuilds/source
	cd builds/source && make package_source

release:
	cmake -H. -Bbuilds/release
	cd builds/release && make
