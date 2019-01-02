
#include <mutex>
#include <string>
#include <sstream>
#include <vector>
#include "swift/shannon_db.h"
#include <bits/stdc++.h>
using namespace std;
using namespace shannon;
std::string kDBPath = "testdb";
std::string dbpath = "/dev/kvdev0";
Status s;
void KVopen(Options &options, std::vector<ColumnFamilyHandle *> *handles, DB **db, int mark)
{
    vector<string> familie_names;
    vector<ColumnFamilyDescriptor> column_families;
    s = DB::ListColumnFamilies(options, kDBPath, dbpath, &familie_names);
    for (auto val : familie_names)
    {
        cout << val << "  : " << mark << endl;
    }
    assert(s.ok());

    for (auto familyname : familie_names)
    {
        column_families.push_back(
            ColumnFamilyDescriptor(familyname, ColumnFamilyOptions()));
    }
    s = DB::Open(options, kDBPath, dbpath, column_families, handles, db);
    assert(s.ok());
}

void KVput(DB *db, string key, string value, ColumnFamilyHandle *handel)
{
    s = db->Put(shannon::WriteOptions(), handel, key, value);
    assert(s.ok());
}

void KVcreate_family(string name)
{
    DB *db;
    Options options;
    options.create_if_missing = true;
    vector<ColumnFamilyHandle *> handles;
    ColumnFamilyHandle *dle;
    KVopen(options, &handles, &db, 2);
    s = db->CreateColumnFamily(options, name, &dle);
    assert(s.ok());
    for (auto handle : handles)
    {
        delete handle;
    }
    delete db;
    delete dle;
}
void KVdrop_handle(DB *db, vector<ColumnFamilyHandle *> *handles)
{
    Status st;
    for (ColumnFamilyHandle *hand : *handles)
    {
        if (hand->GetName() != "default")
        {
            s = db->DropColumnFamily(hand);
            if (!s.ok())
                st = s;
        }
    }
    //assert(s.ok());
}

void KVget(DB *db, vector<ColumnFamilyHandle *> *handles, string &key, string &value, int &i)
{
    s = db->Get(ReadOptions(), (*handles)[i%5], key, &value);
    //assert(s.ok());
    cout << i << " : " << key << " : " << value << endl;
}

void KVforeach (DB *db, const vector<ColumnFamilyHandle *> &handles)
{
    vector<Iterator *> iters;
    s = db->NewIterators(ReadOptions(), handles, &iters);
    //assert(s.ok());
    int i = 0;
    for (auto iter : iters)
    {
        cout << "-----------------------" << endl
             << handles[i%5]->GetName()
             << endl
             << "-----------------------" << endl;
        for (iter->SeekToFirst(); iter->Valid(); iter->Next())
        {
            Slice slice_key = iter->key();
            Slice slice_value = iter->value();
            std::cout << " : " << slice_key.ToString() << " : "
                      << slice_value.ToString() << std::endl;
        }
        ++i;
    }
    for (auto iter : iters)
    {
        delete iter;
    }
}
void KVdelete (DB *db, const vector<ColumnFamilyHandle *> &handles)
{
    vector<Iterator *> iters;
    s = db->NewIterators(ReadOptions(), handles, &iters);
    assert(s.ok());
    int i = 0;
    for (auto iter : iters)
    {
        cout << "-----------------------" << endl
             << handles[i%5]->GetName()
             << endl
             << "-----------------------" << endl;
        for (iter->SeekToFirst(); iter->Valid(); iter->Next())
        {
            Slice slice_key = iter->key();
            Slice slice_value = iter->value();
            db->Delete(WriteOptions(), handles[ i % 5 ] , slice_key);
        }
        ++i;
    }
    for (auto iter : iters)
    {
        delete iter;
    }
}
void KVbatch(DB * db,const vector<ColumnFamilyHandle *> &handles)
{
    WriteBatch batch;
    for (int i = 0; i < 20; ++i) {
        stringstream fs_value;
        stringstream fs_key;
        fs_key << "batch_key" << i;
        fs_value << "batch_val" << i;
        string key = fs_key.str();
        batch.Put(handles[i% 5] ,fs_key.str(),fs_value.str());
    }
        int i = 15;
        stringstream fs_value;
        stringstream fs_key;
        fs_key << "batch_key" << i;
        string key = fs_key.str();
        batch.Delete(handles[i% 5] ,fs_key.str());
        db->Write(WriteOptions(),&batch);
}
int main()
{
    KVcreate_family("new_cf1");
    KVcreate_family("new_cf2");
    KVcreate_family("new_cf3");
    KVcreate_family("new_cf4");
    Options options;
    options.create_if_missing = true;
    DB *db;
    vector<ColumnFamilyHandle *> handles;
    KVopen(options, &handles, &db, 1);
    for (int i = 0; i < 1; ++i)
    {
        stringstream fs_value;
        stringstream fs_key;
        fs_key << "key" << i;
        fs_value << "value" << i;
        KVput(db, fs_key.str(), fs_value.str(), handles[i%5]);
    }
    //db->DropColumnFamily(handles[2]);
    for (int i = 0; i < 10; ++i)
    {
        stringstream fs_value;
        stringstream fs_key;
        fs_key << "key" << i;
        fs_value << "value" << i;
        string key = fs_key.str();
        string value("");
        KVget(db, &handles, key, value, i);
    }
    KVforeach (db, handles);
    //KVdrop_handle(db, &handles);
    cout<<"------batchtest--------"<<endl;
    KVbatch(db, handles);
    KVforeach (db, handles);
    KVdrop_handle(db, &handles);
    delete db;
    for (auto handle : handles)
    {
        delete handle;
    }
    cout << "ok!" << endl;
}
