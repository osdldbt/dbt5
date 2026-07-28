.PHONY: appimage clean debug package release

default:
	@echo "targets: appimage (Linux only), clean, debug, package, release"

appimage:
	cmake -H. -Bbuild/appimage -DCMAKE_INSTALL_PREFIX=/usr
	cd build/appimage && make install DESTDIR=../AppDir
	rm -rf build/AppDir/opt/egen
	mkdir -p build/AppDir/opt/egen
	if [ -d egen/prj ]; then \
		cp -a egen/. build/AppDir/opt/egen; \
	else \
		cp -a build/appimage/_deps/egen-src/. build/AppDir/opt/egen; \
	fi
	build/AppDir/usr/bin/dbt5-build-egen --include-dir=src/include \
			--patch-dir=patches --source-dir=src \
			build/AppDir/opt/egen; \
	cd build/appimage && make appimage

clean:
	-rm -rf build

debug:
	cmake -H. -Bbuild/debug -DCMAKE_BUILD_TYPE=Debug
	cd build/debug && make

package:
	git checkout-index --prefix=build/source/ -a
	cmake -Hbuild/source -Bbuild/package
	cd build/package && make package_source

release:
	cmake -H. -Bbuild/release
	cd build/release && make
