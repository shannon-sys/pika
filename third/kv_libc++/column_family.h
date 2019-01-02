#pragma once

#include "swift/shannon_db.h"
#include "venice_macro.h"
#include <string>
#include <atomic>

namespace shannon{
 class ColumnFamilyHandleImpl : public ColumnFamilyHandle {
  public:
    ColumnFamilyHandleImpl();
    ColumnFamilyHandleImpl(int db_index_, int cf_index_, std::string &name_);
    virtual uint32_t GetID() const override;
    virtual const std::string& GetName() const override;
    virtual Status GetDescriptor(ColumnFamilyDescriptor* desc) override;
    virtual Status SetDescriptor(const ColumnFamilyDescriptor& desc) override;
  private:
    int db_index;
    int cf_index;
    std::string name;
 };
};	//namespace shannon
