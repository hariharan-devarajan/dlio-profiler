//
// Created by haridev on 3/28/23.
//

#ifndef DLIO_PROFILER_CHROME_WRITER_H
#define DLIO_PROFILER_CHROME_WRITER_H

#include <string>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <unistd.h>
#include <hwloc.h>
#include <dlio_profiler/core/constants.h>
#include <atomic>
#include <any>
#include <dlio_profiler/utils/utils.h>
#include <unordered_map>
#include <dlio_profiler/utils/posix_internal.h>
#include <dlio_profiler/utils/configuration_manager.h>
#include <dlio_profiler/core/typedef.h>
#define ERROR(cond, format, ...) \
  DLIO_PROFILER_LOGERROR(format, __VA_ARGS__); \
  if (this->throw_error) assert(cond);
namespace dlio_profiler {
    class ChromeWriter {
    private:
        std::unordered_map<char *, std::any> metadata;
    protected:
        bool throw_error;
        std::string filename;
    private:
        bool enable_core_affinity, include_metadata, enable_compression;
        hwloc_topology_t topology;
        int fd;
        std::atomic_int index;
        char hostname[256];
        static const int MAX_LINE_SIZE=4096;
        static const int MAX_META_LINE_SIZE=3000;
        void convert_json(ConstEventType event_name, ConstEventType category, TimeResolution start_time,
                                                  TimeResolution duration, std::unordered_map<std::string, std::any> *metadata,
                                                  ProcessID process_id, ThreadID thread_id, int* size, char* data);

        bool is_first_write;
        std::mutex write_mtx;
        static const int WRITE_BUFFER_SIZE=1024*1024;
        size_t write_size;
        char* write_buffer;
        inline int write_buffer_op(){
          DLIO_PROFILER_LOGDEBUG("ChromeWriter.write_buffer_op","");
          auto written_elements = dlp_write(fd, write_buffer, write_size);
          if (written_elements != write_size) {  // GCOVR_EXCL_START
            ERROR(written_elements != write_size, "unable to log write %s fd %d for a+ written only %d of %d with error %s",
                  filename.c_str(), fd, written_elements, write_size, strerror(errno));
          }  // GCOVR_EXCL_STOP
          return write_size;
        }
        inline int merge_buffer(const char* data, int size) {
          DLIO_PROFILER_LOGDEBUG("ChromeWriter.merge_buffer","");
          std::lock_guard<std::mutex> lockGuard(write_mtx);
          memcpy(write_buffer + write_size, data, size);
          write_size += size;
          if (write_size >= WRITE_BUFFER_SIZE) {
            write_buffer_op();
            write_size = 0;
          }
          return size;
        }

        std::vector<unsigned> core_affinity() {
          DLIO_PROFILER_LOGDEBUG("ChromeWriter.core_affinity","");
          auto cores = std::vector<unsigned>();
          if (enable_core_affinity) {
            hwloc_cpuset_t set = hwloc_bitmap_alloc();
            hwloc_get_cpubind(topology, set, HWLOC_CPUBIND_PROCESS);
            for (unsigned id = hwloc_bitmap_first(set); id != -1; id = hwloc_bitmap_next(set, id)) {
              cores.push_back(id);
            }
            hwloc_bitmap_free(set);
          }
          return cores;
        }

        void get_hostname(char* hostname) {
          DLIO_PROFILER_LOGDEBUG("ChromeWriter.get_hostname","");
          gethostname(hostname, 256);
        }

    public:
        ChromeWriter(): is_first_write(true), fd(-1), write_mtx(), enable_core_affinity(false), include_metadata(false),
                  enable_compression(false), index(0), write_size(0){
          DLIO_PROFILER_LOGDEBUG("ChromeWriter.ChromeWriter","");
          write_buffer = static_cast<char *>(malloc(WRITE_BUFFER_SIZE + MAX_LINE_SIZE));
          get_hostname(hostname);
          auto conf = dlio_profiler::Singleton<dlio_profiler::ConfigurationManager>::get_instance();
          include_metadata = conf->metadata;
          enable_core_affinity = conf->core_affinity;
          enable_compression = conf->compression;
          if (enable_core_affinity) {
            hwloc_topology_init(&topology);  // initialization
            hwloc_topology_load(topology);   // actual detection
          }
        }
        ~ChromeWriter(){DLIO_PROFILER_LOGDEBUG("Destructing ChromeWriter","");}
        void initialize(char *filename, bool throw_error);

        void log(ConstEventType event_name, ConstEventType category, TimeResolution &start_time, TimeResolution &duration,
                 std::unordered_map<std::string, std::any> *metadata, ProcessID process_id, ThreadID tid);

        void finalize();
    };
}

#endif //DLIO_PROFILER_CHROME_WRITER_H
