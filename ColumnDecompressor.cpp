#include  <boost/lexical_cast.hpp>
#include <boost/iostreams/filter/bzip2.hpp>
#include <boost/iostreams/device/file.hpp>
#include <boost/algorithm/string/split.hpp>
#include "ColumnDecompressor.h"
#include "StreamFlusher.h"
ColumnDecompressor::ColumnDecompressor(char delimiter, size_t nCacheSize)
    :mCacheSize(nCacheSize)
    ,mDelimiter(delimiter)
{
}

ColumnDecompressor::~ColumnDecompressor()
{
}

ColumnDecompressor::RowCache::RowCache()
{
    Fields[0] = &Data[0];
    ColumnCount = 0;
}

void ColumnDecompressor::RowCache::WriteOut(bio::filtering_ostream& output, std::array<size_t, MAX_COLUMNS>& columnIndices)
{
    for(size_t iColumn = 0; iColumn < ColumnCount - 1; ++iColumn)
    {
        size_t index = columnIndices[iColumn];
        size_t size = Fields[index + 1] - Fields[index];
        output.write(Fields[index], size);
    }

    size_t lastIndex = columnIndices[ColumnCount - 1];
    size_t lastSize = Fields[lastIndex + 1] - Fields[lastIndex];
    output.write(Fields[lastIndex], lastSize - 1);
    output.write(NEWLINE, 1);
    ColumnCount = 0;
}

namespace bal = boost::algorithm;
void ColumnDecompressor::Decompress(const std::string& inputFileName, const std::string& outputFileName)
{
    bio::filtering_istream inputStream;
    inputStream.push(bio::bzip2_decompressor());
    inputStream.push(bio::file_source(inputFileName));

#if 0
    //remove
    int j(0);
    std::string l;
    while(std::getline(inputStream, l))
    {
	std::cout << (j++) << l << std::endl;
    }
    std::cout << j << std::endl;
    return;
#endif
    //decompress header
    std::string header;
    std::getline(inputStream, header);

    std::vector<boost::iterator_range<std::string::iterator>> columns;
    bal::split(columns, header, [this](auto c) { return c == mDelimiter; }, bal::token_compress_off);

    bio::filtering_ostream outputStream;
    outputStream.push(bio::file_sink(outputFileName));
    StreamFlusher<bio::filtering_ostream> flusher(outputStream);
    outputStream << header << NEWLINE[0];

    //decompress meta
    size_t nTotalColumns = columns.size();
    std::string sMeta;
    if(!std::getline(inputStream, sMeta))
    {
        return;
    }

    std::vector<std::string> vMetas;
    bal::split(vMetas, sMeta, [this](auto c) {return c == mDelimiter;}, bal::token_compress_off);
    std::array<size_t, MAX_COLUMNS> columnIndices;
    std::array<std::pair<size_t, size_t> , MAX_COLUMNS> METAS;
    std::array<std::pair<size_t, size_t>* , MAX_COLUMNS> metas;
    for(size_t iColumn = 0; iColumn < vMetas.size(); ++iColumn)
    {
        METAS[iColumn].first = boost::lexical_cast<size_t>(vMetas[iColumn]);
        METAS[iColumn].second = iColumn;
	metas[iColumn] = &METAS[iColumn];
    }

    std::sort(metas.begin(), metas.begin() + vMetas.size(), [this](auto l, auto r){ return l->first < r->first; });
    for(size_t iColumn = 0; iColumn < vMetas.size(); ++iColumn)
    {
        columnIndices[iColumn] = metas[iColumn]->second;
    }

    //compress others
    bool needWrite = false;
    std::string column;
    size_t nCurrentColumn = 0;
    size_t nRowCount = 0;
    while(std::getline(inputStream, column))
    {
	if(0 == column.size())
	{
	    continue;
	}

        std::vector<boost::iterator_range<std::string::iterator>> rows;
        bal::split(rows, column, [this](auto c) {return c == mDelimiter;}, bal::token_compress_off);
        nRowCount = std::max(nRowCount, rows.size() - 1);
        Append2Rows(rows, nCurrentColumn);
        ++nCurrentColumn;
        needWrite = true;
        if(nCurrentColumn == nTotalColumns)
        {
            WriteOutCache(outputStream, columnIndices, nRowCount);
            needWrite = false;
	    nRowCount = 0;
      	    nCurrentColumn = 0;
        }
    }

    if(needWrite)
    {
        WriteOutCache(outputStream, columnIndices, nRowCount);
    }
}

void ColumnDecompressor::Append2Rows(std::vector<boost::iterator_range<std::string::iterator>>& column, size_t nColumnIndex)
{
    size_t rowCount = column.size() - 1; //最后一个为,没有内容
    if(mRows.size() < rowCount)
    {
        for(size_t iRow = mRows.size(); iRow < rowCount; ++iRow)
        {
            mRows.push_back(std::make_shared<RowCache>());
        }
    }

    for(size_t iRow = 0; iRow < rowCount; ++iRow)
    {
        auto& rowCache = *mRows[iRow];
        auto size = column[iRow].size() + 1;
	if(size > MAX_ROW_SIZE)
	{
	    throw std::logic_error("line two large");
	}

        auto pStart = rowCache.Fields[nColumnIndex];
        for(auto it = column[iRow].begin(); it != column[iRow].end() + 1; ++it)
        {
            *pStart = *it;
            ++pStart;
        }
        rowCache.Fields[nColumnIndex + 1] = pStart;
        rowCache.ColumnCount++;
    }
}


void ColumnDecompressor::WriteOutCache(bio::filtering_ostream& output, std::array<size_t, MAX_COLUMNS>& columnIndices, size_t nRowCount)
{
    for(size_t iRow = 0; iRow < nRowCount; ++iRow)
    {
        mRows[iRow]->WriteOut(output, columnIndices);
    }
}
