#include <arrow/filesystem/api.h>
#include <arrow/json/api.h>
#include <arrow/memory_pool.h>
#include <spdlog/spdlog.h>

#include <iostream>

int main() {
  auto logger = spdlog::default_logger();
  arrow::Status st;
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  logger->info("Create fs");
  auto fs =
      arrow::fs::FileSystemFromUri("file:///home/kabbe/Code/CPP/arrow_json")
          .ValueOrDie();
  logger->info("Open file");
  auto file = fs->OpenInputFile("/home/kabbe/Code/CPP/arrow_json/test.json")
                  .ValueOrDie();

  auto read_options = arrow::json::ReadOptions::Defaults();
  auto parse_options = arrow::json::ParseOptions::Defaults();

  // Instantiate TableReader from input stream and options
  // std::shared_ptr<arrow::json::TableReader> reader;
  auto reader =
      arrow::json::TableReader::Make(pool, file, read_options, parse_options)
          .ValueOrDie();

  std::shared_ptr<arrow::Table> table;
  // Read table from JSON file
  logger->info("Reading JSON");
}