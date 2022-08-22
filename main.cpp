#include <arrow/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/json/api.h>
#include <arrow/memory_pool.h>

#include <iostream>

int main() {
  // Copied from https://arrow.apache.org/docs/cpp/json.html#basic-usage
  arrow::Status st;
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  auto fs =
      arrow::fs::FileSystemFromUri("file:///home/kabbe/Code/CPP/arrow_json")
          .ValueOrDie();
  auto file = fs->OpenInputFile("/home/kabbe/Code/CPP/arrow_json/test.json")
                  .ValueOrDie();

  auto read_options = arrow::json::ReadOptions::Defaults();
  auto parse_options = arrow::json::ParseOptions::Defaults();

  auto reader =
      arrow::json::TableReader::Make(pool, file, read_options, parse_options)
          .ValueOrDie();

  auto table = reader->Read().ValueOrDie();

  auto b = std::static_pointer_cast<arrow::MapArray>(
      table->GetColumnByName("b")->chunk(0));

  auto b_flat =
      std::static_pointer_cast<arrow::ListArray>(b->Flatten().ValueOrDie());

  std::cout << "length b_flat: " << b_flat->length() << "\n";

  for (int i = 0; i < b_flat->length(); i++) {
    auto val = std::static_pointer_cast<arrow::StructScalar>(
        b_flat->GetScalar(i).ValueOrDie());
    arrow::Int64Builder builder;
    auto x = std::static_pointer_cast<arrow::Int64Scalar>(
        val->field("x").ValueOrDie());
    builder.AppendScalar(*x);
    int64_t value = builder.GetValue(0);
    std::cout << "x = " << value << "\n";
  }
}
