****************
Example Programs
****************

------------
C++ Example
------------

Application Level Example:
**************************

.. code-block:: c
   :linenos:

    #include <dlio_profiler/dlio_profiler.h>

    void foo() {
      DLIO_PROFILER_CPP_FUNCTION(); // Add at the begining of each function
      sleep(1);
      {
        DLIO_PROFILER_CPP_REGION(CUSTOM); // Add at the beginning of code block. keep name unique
        sleep(1);
        DLIO_PROFILER_CPP_REGION_START(CUSTOM_BLOCK); // add start. keep name unique
        sleep(1);
        DLIO_PROFILER_CPP_REGION_END(CUSTOM_BLOCK); // add end. Match name from START.
      }
    }

    int main(int argc, char *argv[]) {
      // Basic Bookkeeping
      int init = 0;
      if (argc > 2) {
        if (strcmp(argv[2], "1") == 0) {
          // Initialize Application Profiler
          DLIO_PROFILER_CPP_INIT(nullptr, nullptr, nullptr);
          init = 1;
        }
      }
      char filename[1024];
      sprintf(filename, "%s/demofile.txt", argv[1]);

      // Run functions
      foo();
      // Implicit I/O calls No need for marking.
      FILE *fh = fopen(filename, "w+");
      if (fh != NULL) {
        fwrite("hello", sizeof("hello"), 1, fh);
        fclose(fh);
      }
      if (init == 1) {
        // Finalize Application Profiler
        DLIO_PROFILER_CPP_FINI();
      }
      return 0;
    }

For this example, link with libdlio_profiler.so at compile time.
As the DLIO_PROFILER_CPP_INIT do not pass log file or data dir, we need to set ``DLIO_PROFILER_LOG_FILE`` and ``DLIO_PROFILER_DATA_DIR``.
By default the DLIO Profiler mode is set to FUNCTION.
Example of running this configurations are:

.. code-block:: bash
   :linenos:

    # the process id, app_name and .pfw will be appended by the profiler for each app and process.
    # name of final log file is ~/log_file-<APP_NAME>-<PID>.pfw
    DLIO_PROFILER_LOG_FILE=~/log_file
    # Colon separated paths for including for profiler
    DLIO_PROFILER_DATA_DIR=/dev/shm/:/p/gpfs1/$USER/dataset
    # Enable profiler
    DLIO_PROFILER_ENABLE=1


LD_PRELOAD Example:
**************************

.. code-block:: c
   :linenos:

    #include <dlio_profiler/dlio_profiler.h>

    int main(int argc, char *argv[]) {
      char filename[1024];
      sprintf(filename, "%s/demofile.txt", argv[1]);
      foo(); # function will be ignored in pure LD_PRELOAD mode.
      // Implicit I/O calls No need for marking.
      FILE *fh = fopen(filename, "w+");
      if (fh != NULL) {
        fwrite("hello", sizeof("hello"), 1, fh);
        fclose(fh);
      }
      return 0;
    }

For this example, LD_PRELOAD the executable with libdlio_profiler_preload.so at runtime.
We need to set ``DLIO_PROFILER_LOG_FILE`` and ``DLIO_PROFILER_DATA_DIR``.
Example of running this configurations are:

.. code-block:: bash
   :linenos:

    # the process id, app_name and .pfw will be appended by the profiler for each app and process.
    # name of final log file is ~/log_file-<APP_NAME>-<PID>.pfw
    export DLIO_PROFILER_LOG_FILE=~/log_file
    # Colon separated paths for including for profiler
    export DLIO_PROFILER_DATA_DIR=/dev/shm/:/p/gpfs1/$USER/dataset
    # Set the mode to PRELOAD
    export DLIO_PROFILER_INIT=PRELOAD
    # Enable profiler
    export DLIO_PROFILER_ENABLE=1


Hybrid Example:
**************************

.. code-block:: c
   :linenos:

    #include <dlio_profiler/dlio_profiler.h>

    void foo() {
      DLIO_PROFILER_CPP_FUNCTION(); // Add at the begining of each function
      sleep(1);
      {
        DLIO_PROFILER_CPP_REGION(CUSTOM); // Add at the beginning of code block. keep name unique
        sleep(1);
        DLIO_PROFILER_CPP_REGION_START(CUSTOM_BLOCK); // add start. keep name unique
        sleep(1);
        DLIO_PROFILER_CPP_REGION_END(CUSTOM_BLOCK); // add end. Match name from START.
      }
    }

    int main(int argc, char *argv[]) {
      // Basic Bookkeeping
      int init = 0;
      if (argc > 2) {
        if (strcmp(argv[2], "1") == 0) {
          // Initialize Application Profiler
          DLIO_PROFILER_CPP_INIT(nullptr, nullptr, nullptr);
          init = 1;
        }
      }
      char filename[1024];
      sprintf(filename, "%s/demofile.txt", argv[1]);

      // Run functions
      foo();
      // Implicit I/O calls No need for marking.
      FILE *fh = fopen(filename, "w+");
      if (fh != NULL) {
        fwrite("hello", sizeof("hello"), 1, fh);
        fclose(fh);
      }
      if (init == 1) {
        // Finalize Application Profiler
        DLIO_PROFILER_CPP_FINI();
      }
      return 0;
    }

For this example, link with libdlio_profiler.so at compile time and LD_PRELOAD the executable with libdlio_profiler_preload.soat runtime.
As the DLIO_PROFILER_CPP_INIT do not pass log file or data dir, we need to set ``DLIO_PROFILER_LOG_FILE`` and ``DLIO_PROFILER_DATA_DIR``.
By default the DLIO Profiler mode is set to FUNCTION.
Example of running this configurations are:

.. code-block:: bash
   :linenos:

    # the process id, app_name and .pfw will be appended by the profiler for each app and process.
    # name of final log file is ~/log_file-<APP_NAME>-<PID>.pfw
    DLIO_PROFILER_LOG_FILE=~/log_file
    # Colon separated paths for including for profiler
    DLIO_PROFILER_DATA_DIR=/dev/shm/:/p/gpfs1/$USER/dataset
    # Set the mode to PRELOAD
    export DLIO_PROFILER_INIT=PRELOAD
    # Enable profiler
    DLIO_PROFILER_ENABLE=1

------------
C Example
------------

Application Level Example:
**************************

.. code-block:: c
   :linenos:

    #include <dlio_profiler/dlio_profiler.h>

    void foo() {
      DLIO_PROFILER_C_FUNCTION_START();
      sleep(1);
      if (<CONDITION>) {
        DLIO_PROFILER_C_FUNCTION_END();
        return; // Define DLIO_PROFILER_C_FUNCTION_END on every branch
      }
      {
        DLIO_PROFILER_C_REGION_START(CUSTOM);
        sleep(1);
        DLIO_PROFILER_C_REGION_END(CUSTOM); // END region CUSTOM.
      }
      DLIO_PROFILER_C_FUNCTION_END(); // Define DLIO_PROFILER_C_FUNCTION_END on every branch
    }

    int main(int argc, char *argv[]) {
      // Basic Bookkeeping
      int init = 0;
      if (argc > 2) {
        if (strcmp(argv[2], "1") == 0) {
          // Initialize Application Profiler
          DLIO_PROFILER_C_INIT(nullptr, nullptr, nullptr);
          init = 1;
        }
      }
      char filename[1024];
      sprintf(filename, "%s/demofile.txt", argv[1]);

      // Run functions
      foo();
      // Implicit I/O calls No need for marking.
      FILE *fh = fopen(filename, "w+");
      if (fh != NULL) {
        fwrite("hello", sizeof("hello"), 1, fh);
        fclose(fh);
      }
      if (init == 1) {
        // Finalize Application Profiler
        DLIO_PROFILER_C_FINI();
      }
      return 0;
    }

For this example, link with libdlio_profiler.so at compile time.
As the DLIO_PROFILER_CPP_INIT do not pass log file or data dir, we need to set ``DLIO_PROFILER_LOG_FILE`` and ``DLIO_PROFILER_DATA_DIR``.
By default the DLIO Profiler mode is set to FUNCTION.
Example of running this configurations are:

.. code-block:: bash
   :linenos:

    # the process id, app_name and .pfw will be appended by the profiler for each app and process.
    # name of final log file is ~/log_file-<APP_NAME>-<PID>.pfw
    DLIO_PROFILER_LOG_FILE=~/log_file
    # Colon separated paths for including for profiler
    DLIO_PROFILER_DATA_DIR=/dev/shm/:/p/gpfs1/$USER/dataset
    # Enable profiler
    DLIO_PROFILER_ENABLE=1


LD_PRELOAD Example:
**************************

.. code-block:: c
   :linenos:

    #include <dlio_profiler/dlio_profiler.h>

    int main(int argc, char *argv[]) {
      char filename[1024];
      sprintf(filename, "%s/demofile.txt", argv[1]);
      foo(); # function will be ignored in pure LD_PRELOAD mode.
      // Implicit I/O calls No need for marking.
      FILE *fh = fopen(filename, "w+");
      if (fh != NULL) {
        fwrite("hello", sizeof("hello"), 1, fh);
        fclose(fh);
      }
      return 0;
    }

For this example, LD_PRELOAD the executable with libdlio_profiler_preload.so at runtime.
We need to set ``DLIO_PROFILER_LOG_FILE`` and ``DLIO_PROFILER_DATA_DIR``.
Example of running this configurations are:

.. code-block:: bash
   :linenos:

    # the process id, app_name and .pfw will be appended by the profiler for each app and process.
    # name of final log file is ~/log_file-<APP_NAME>-<PID>.pfw
    export DLIO_PROFILER_LOG_FILE=~/log_file
    # Colon separated paths for including for profiler
    export DLIO_PROFILER_DATA_DIR=/dev/shm/:/p/gpfs1/$USER/dataset
    # Set the mode to PRELOAD
    export DLIO_PROFILER_INIT=PRELOAD
    # Enable profiler
    export DLIO_PROFILER_ENABLE=1


Hybrid Example:
**************************

.. code-block:: c
   :linenos:

    #include <dlio_profiler/dlio_profiler.h>

    void foo() {
      DLIO_PROFILER_C_FUNCTION_START();
      sleep(1);
      if (<CONDITION>) {
        DLIO_PROFILER_C_FUNCTION_END();
        return; // Define DLIO_PROFILER_C_FUNCTION_END on every branch
      }
      {
        DLIO_PROFILER_C_REGION_START(CUSTOM);
        sleep(1);
        DLIO_PROFILER_C_REGION_END(CUSTOM); // END region CUSTOM.
      }
      DLIO_PROFILER_C_FUNCTION_END(); // Define DLIO_PROFILER_C_FUNCTION_END on every branch
    }

    int main(int argc, char *argv[]) {
      // Basic Bookkeeping
      int init = 0;
      if (argc > 2) {
        if (strcmp(argv[2], "1") == 0) {
          // Initialize Application Profiler
          DLIO_PROFILER_C_INIT(nullptr, nullptr, nullptr);
          init = 1;
        }
      }
      char filename[1024];
      sprintf(filename, "%s/demofile.txt", argv[1]);

      // Run functions
      foo();
      // Implicit I/O calls No need for marking.
      FILE *fh = fopen(filename, "w+");
      if (fh != NULL) {
        fwrite("hello", sizeof("hello"), 1, fh);
        fclose(fh);
      }
      if (init == 1) {
        // Finalize Application Profiler
        DLIO_PROFILER_C_FINI();
      }
      return 0;
    }

For this example, link with libdlio_profiler.so at compile time and LD_PRELOAD the executable with libdlio_profiler_preload.soat runtime.
As the DLIO_PROFILER_CPP_INIT do not pass log file or data dir, we need to set ``DLIO_PROFILER_LOG_FILE`` and ``DLIO_PROFILER_DATA_DIR``.
By default the DLIO Profiler mode is set to FUNCTION.
Example of running this configurations are:

.. code-block:: bash
   :linenos:

    # the process id, app_name and .pfw will be appended by the profiler for each app and process.
    # name of final log file is ~/log_file-<APP_NAME>-<PID>.pfw
    DLIO_PROFILER_LOG_FILE=~/log_file
    # Colon separated paths for including for profiler
    DLIO_PROFILER_DATA_DIR=/dev/shm/:/p/gpfs1/$USER/dataset
    # Set the mode to PRELOAD
    export DLIO_PROFILER_INIT=PRELOAD
    # Enable profiler
    DLIO_PROFILER_ENABLE=1



----------------
Python Example
----------------

Application Level Example:
**************************

.. code-block:: python
   :linenos:

    from dlio_profiler.logger import dlio_logger, fn_interceptor
    log_inst = dlio_logger.initialize_log(logfile=None, data_dir=None, process_id=-1)
    dlio_log = fn_interceptor("COMPUTE")

    # Example of using function decorators
    @dlio_log.log
    def log_events(index):
        sleep(1)

    # Example of function spawning and implicit I/O calls
    def posix_calls(val):
        index, is_spawn = val
        path = f"{cwd}/data/demofile{index}.txt"
        f = open(path, "w+")
        f.write("Now the file has more content!")
        f.close()
        if is_spawn:
            print(f"Calling spawn on {index} with pid {os.getpid()}")
            log_inst.finalize() # This need to be called to correctly finalize DLIO Profiler.
        else:
            print(f"Not calling spawn on {index} with pid {os.getpid()}")

    # NPZ calls internally calls POSIX calls.
    def npz_calls(index):
        # print(f"{cwd}/data/demofile2.npz")
        path = f"{cwd}/data/demofile{index}.npz"
        if os.path.exists(path):
            os.remove(path)
        records = np.random.randint(255, size=(8, 8, 1024), dtype=np.uint8)
        record_labels = [0] * 1024
        np.savez(path, x=records, y=record_labels)

    def main():
        log_events(0)
        npz_calls(1)
        with get_context('spawn').Pool(1, initializer=init) as pool:
            pool.map(posix_calls, ((2, True),))
        log_inst.finalize()


    if __name__ == "__main__":
        main()

For this example, as the DLIO_PROFILER_CPP_INIT do not pass log file or data dir, we need to set ``DLIO_PROFILER_LOG_FILE`` and ``DLIO_PROFILER_DATA_DIR``.
By default the DLIO Profiler mode is set to FUNCTION.
Example of running this configurations are:

.. code-block:: bash
   :linenos:

    # the process id, app_name and .pfw will be appended by the profiler for each app and process.
    # name of final log file is ~/log_file-<APP_NAME>-<PID>.pfw
    DLIO_PROFILER_LOG_FILE=~/log_file
    # Colon separated paths for including for profiler
    DLIO_PROFILER_DATA_DIR=/dev/shm/:/p/gpfs1/$USER/dataset:$PWD/data
    # Enable profiler
    DLIO_PROFILER_ENABLE=1


LD_PRELOAD Example:
*******************

.. code-block:: python
   :linenos:

    # Example of function spawning and implicit I/O calls
    def posix_calls(val):
        index, is_spawn = val
        path = f"{cwd}/data/demofile{index}.txt"
        f = open(path, "w+")
        f.write("Now the file has more content!")
        f.close()
        if is_spawn:
            print(f"Calling spawn on {index} with pid {os.getpid()}")
        else:
            print(f"Not calling spawn on {index} with pid {os.getpid()}")

    # NPZ calls internally calls POSIX calls.
    def npz_calls(index):
        # print(f"{cwd}/data/demofile2.npz")
        path = f"{cwd}/data/demofile{index}.npz"
        if os.path.exists(path):
            os.remove(path)
        records = np.random.randint(255, size=(8, 8, 1024), dtype=np.uint8)
        record_labels = [0] * 1024
        np.savez(path, x=records, y=record_labels)

    def main():
        npz_calls(1)
        with get_context('spawn').Pool(1, initializer=init) as pool:
            pool.map(posix_calls, ((2, True),))

    if __name__ == "__main__":
        main()

For this example, LD_PRELOAD the executable with libdlio_profiler_preload.so at runtime.
We need to set ``DLIO_PROFILER_LOG_FILE`` and ``DLIO_PROFILER_DATA_DIR``.
Example of running this configurations are:

.. code-block:: bash
   :linenos:

    # the process id, app_name and .pfw will be appended by the profiler for each app and process.
    # name of final log file is ~/log_file-<APP_NAME>-<PID>.pfw
    export DLIO_PROFILER_LOG_FILE=~/log_file
    # Colon separated paths for including for profiler
    export DLIO_PROFILER_DATA_DIR=/dev/shm/:/p/gpfs1/$USER/dataset
    # Set the mode to PRELOAD
    export DLIO_PROFILER_INIT=PRELOAD
    # Enable profiler
    export DLIO_PROFILER_ENABLE=1


Hybrid Example:
**************************


.. code-block:: python
   :linenos:

    from dlio_profiler.logger import dlio_logger, fn_interceptor
    log_inst = dlio_logger.initialize_log(logfile=None, data_dir=None, process_id=-1)
    dlio_log = fn_interceptor("COMPUTE")

    # Example of using function decorators
    @dlio_log.log
    def log_events(index):
        sleep(1)

    # Example of function spawning and implicit I/O calls
    def posix_calls(val):
        index, is_spawn = val
        path = f"{cwd}/data/demofile{index}.txt"
        f = open(path, "w+")
        f.write("Now the file has more content!")
        f.close()
        if is_spawn:
            print(f"Calling spawn on {index} with pid {os.getpid()}")
            log_inst.finalize() # This need to be called to correctly finalize DLIO Profiler.
        else:
            print(f"Not calling spawn on {index} with pid {os.getpid()}")

    # NPZ calls internally calls POSIX calls.
    def npz_calls(index):
        # print(f"{cwd}/data/demofile2.npz")
        path = f"{cwd}/data/demofile{index}.npz"
        if os.path.exists(path):
            os.remove(path)
        records = np.random.randint(255, size=(8, 8, 1024), dtype=np.uint8)
        record_labels = [0] * 1024
        np.savez(path, x=records, y=record_labels)

    def main():
        log_events(0)
        npz_calls(1)
        with get_context('spawn').Pool(1, initializer=init) as pool:
            pool.map(posix_calls, ((2, True),))
        log_inst.finalize()


    if __name__ == "__main__":
        main()

For this example, use LD_PRELOAD the executable with libdlio_profiler_preload.soat runtime.
As the DLIO_PROFILER_CPP_INIT do not pass log file or data dir, we need to set ``DLIO_PROFILER_LOG_FILE`` and ``DLIO_PROFILER_DATA_DIR``.
By default the DLIO Profiler mode is set to FUNCTION.
Example of running this configurations are:

.. code-block:: bash
   :linenos:

    # the process id, app_name and .pfw will be appended by the profiler for each app and process.
    # name of final log file is ~/log_file-<APP_NAME>-<PID>.pfw
    DLIO_PROFILER_LOG_FILE=~/log_file
    # Colon separated paths for including for profiler
    DLIO_PROFILER_DATA_DIR=/dev/shm/:/p/gpfs1/$USER/dataset
    # Set the mode to PRELOAD
    export DLIO_PROFILER_INIT=PRELOAD
    # Enable profiler
    DLIO_PROFILER_ENABLE=1