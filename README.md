# metabase-guandata-driver

Guandata driver for Metabase BI

Version compatibility:

## How to use

1.  Download the guandata.metabase-driver.jar from releases
2.  Put it under metabase's plugins folder (plugins folder is at the same parent folder with metabase.jar)
3.  Restart metabase


## Building the driver

### Prereq: Build Metabase source locally

Please refer to [Building Metabase](https://www.metabase.com/docs/latest/developers-guide/build.html) document.

### Build metabase-guandata-driver

clone the source code, and put it under the same parent folder as metabase's source code.

then, under this metabase-guandata-driver folder, run

```shell
./build.sh
```

The generated "guandata.metabase-driver.jar" can be found in target folder
