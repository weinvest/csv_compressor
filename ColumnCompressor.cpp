#include <boost/iostreams/filter/bzip2.hpp>
#include <boost/iostreams/device/file.hpp>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp> 
#include "ColumnCompressor.h"
namespace bip = boost::interprocess;
ColumnCompressor::ColumnCompressor(char delimiter, size_t nBlockRows)
    :mBlockRows(nBlockRows)
    ,mTotalColumns(0)
    ,mTotalRows(0)
    ,mCurrentRowsInBlock(0)
    ,mDelimiter(delimiter)
{
    mNewLine[0] = '\n';
}

void ColumnCompressor::Compress(const std::string& inputFileName, const std::string& outputFileName)
{
    //将输入文件映射到内存
    bip::file_mapping inputFile(inputFileName.c_str(), bip::read_only);  
    bip::mapped_region inputFileRegion(inputFile, bip::read_write);  
	  
    const char* startAddress = static_cast<const char*>(inputFileRegion.get_address());  
    auto size  = inputFileRegion.get_size(); 
    const char* endAddress = startAddress + size;
    auto currentAddress = startAddress;
    
    if(currentAddress == endAddress)
    {
	return; //空文件
    }

    //初始化输出流
    mOutputStream.reset(new bio::filtering_ostream());
    mOutputStream->push(bio::bzip2_decompressor());
    mOutputStream->push(bio::file_sink(outputFileName));

    //读入header
    Columns columns;
    bool hasMoreData = Next(currentAddress, endAddress, columns, mTotalColumns);

    //写入元信息
    mOutputStream->write(reinterpret_cast<const char*>(&mTotalColumns), sizeof(mTotalColumns) + sizeof(mBlockRows));
    mOutputStream->write(mNewLine, 1);

    //写入文件头
    mOutputStream->write(startAddress, currentAddress - startAddress + 1);

    if(!hasMoreData)
    {
	return;
    }

    //读入第一行
    size_t columnCount(0);
    hasMoreData = Next(currentAddress, endAddress, columns, columnCount);

    //开始每mBlockRows一压缩
}

bool ColumnCompressor::Next(const char*& currentAddress, const char* endAddress, Columns& spliters, size_t& columns)
{
    columns = 0;
    spliters[0] = ++currentAddress;
    while(currentAddress != endAddress && *currentAddress != mNewLine[0])
    {
        if(*currentAddress == mDelimiter)
        {
            spliters[++columns] = currentAddress + 1;
        }
        ++currentAddress;
    }
    spliters[columns] = currentAddress;

    return currentAddress != endAddress;
}
