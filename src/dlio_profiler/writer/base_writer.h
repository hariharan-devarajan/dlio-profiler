//
// Created by haridev on 3/28/23.
//

#ifndef DLIO_PROFILER_BASE_WRITER_H
#define DLIO_PROFILER_BASE_WRITER_H

#include <unordered_map>
#include <any>
#include <dlio_profiler/core/common.h>

namespace dlio_profiler {
    class BaseWriter {
    private:
        std::unordered_map<char *, std::any> metadata;
    protected:
        bool throw_error;
        std::string filename;
    public:
        virtual void initialize(char *filename, bool throw_error) = 0;

        virtual void log(std::string &event_name, std::string &category,
                         TimeResolution &start_time, TimeResolution &duration,
                         std::unordered_map<std::string, std::any> &metadata, int process_id) = 0;

        virtual void finalize() = 0;

    };
}
#endif //DLIO_PROFILER_BASE_WRITER_H
