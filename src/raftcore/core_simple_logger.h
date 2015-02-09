#ifndef CORE_SIMPLE_LOGGER_H_
#define CORE_SIMPLE_LOGGER_H_

#include <boost/log/trivial.hpp>
#include <boost/log/sources/global_logger_storage.hpp>

#define LOGFILE "raft.log"
// just log messages with severity >= SEVERITY_THRESHOLD are written
#define SEVERITY_THRESHOLD logging::trivial::info

// register a global logger
BOOST_LOG_GLOBAL_LOGGER(logger, boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>)

// just a helper macro used by the macros below - don't use it in your code
#define BOOST_GLOBAL_LOG(severity) BOOST_LOG_SEV(logger::get(),boost::log::trivial::severity)
#define LOG_TRACE   BOOST_GLOBAL_LOG(trace)
#define LOG_DEBUG   BOOST_GLOBAL_LOG(debug)
#define LOG_INFO    BOOST_GLOBAL_LOG(info)
#define LOG_WARNING BOOST_GLOBAL_LOG(warning)
#define LOG_ERROR   BOOST_GLOBAL_LOG(error)
#define LOG_FATAL   BOOST_GLOBAL_LOG(fatal)

#endif