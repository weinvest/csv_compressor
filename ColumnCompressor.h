#ifndef COLUMNCOMPRESSOR_H
#define COLUMNCOMPRESSOR_H
#include <sstream>
#include <memory>
#include <array>
#include <boost/iostreams/filtering_stream.hpp>
static const size_t MAX_COLUMNS = 512;
namespace bio = boost::iostreams;
class ColumnCompressor
{
public:
    typedef std::array<const char*, MAX_COLUMNS> Columns;
    ColumnCompressor(char delimiter, size_t nBlockRows);

    void Compress(const std::string& inputFileName, const std::string& outputFileName);
private:

    bool Next(const char*& currentAddress,const char* endAddress, Columns& spliters, size_t& columns);


    //下面这两个变量的位置不能改变
    const size_t mBlockRows;
    size_t mTotalColumns;

    size_t mTotalRows;
    size_t mCurrentRowsInBlock;

    struct ColumnInfo
    {
	enum type
	{
	    INT, DOUBLE, STRING
	}Type;
	std::stringstream ColumnStream;
    };

    std::shared_ptr<bio::filtering_ostream> mOutputStream;
    char mDelimiter;
    char mNewLine[1];
};

#endif // COLUMNCOMPRESSOR_H
