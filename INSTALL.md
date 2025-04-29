## Building from the Source
Follow these steps to build AgensGraph from the source code:
1. **Clone the Repository**
    ```sh
    git clone https://github.com/skaiworldwide-oss/agensgraph.git
    ```

2. **Install Dependencies**
    - **Rocky Linux**:
        ```sh
        yum install gcc glibc readline readline-devel zlib zlib-devel perl
        ```

    - **Fedora**:
        ```sh
        dnf install gcc glibc bison flex readline readline-devel zlib zlib-devel
        ```

    - **RHEL**:
        ```sh
        yum install gcc glibc glib-common readline readline-devel zlib zlib-devel flex bison
        ```

    - **Ubuntu**:
        ```sh
        sudo apt-get install build-essential libreadline-dev zlib1g-dev flex bison
        ```

    - **macOS** (install Xcode):
        ```bash
        xcode-select --install
        ```

3.  **Configure the Source Tree**
    ```sh
    ./configure --prefix=$(pwd)
    ```
    > By default, `make install` installs files in `/usr/local/pgsql/bin`, `/usr/local/pgsql/lib`, etc. Specify an installation prefix to the current directory. If `configure` encounters missing headers, use `--with-includes=/path/to/headers`.

4. **Build & install AgensGraph**:
    ```sh
    make install
    ```

5. **Set Up Environment Variables**
    - Add the install path to the `PATH` environment variable:
        ```sh
        . ag-env.sh
        ```
      OR, edit your `/.bashrc` file (`/.bash_profile` on macOS):
        ```sh
        echo "export PATH=/path/to/agensgraph/bin:\$PATH" >> ~/.bashrc
        echo "export LD_LIBRARY_PATH=/path/to/agensgraph/lib:\$LD_LIBRARY_PATH" >> ~/.bashrc
        ```
6. **Optional: Build and Install with Additional Modules**
    ```sh
    make install-world
    ```
7. **Optional: Set `AGDATA` Environment Variable**
    ```sh
    echo "export AGDATA=/path/to/agensgraph/data" >> ~/.bashrc
    ```
