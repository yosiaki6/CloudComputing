#include <iostream>
#include <cstdlib>
#include <exception>
using namespace std;
#include <map>
#include <string>
#include <list>

#include "json/json.h"
#include "json/writer.h"
#include "json/reader.h"

map<string, string> month;

/**
* split関数
* @param string str 分割したい文字列
* @param string delim デリミタ
* @return list<string> 分割された文字列
*/
void split(string str, string delim, string* result)
{
    int cutAt;
    int index = 0;
    while( (cutAt = str.find_first_of(delim)) != str.npos )
    {
        if(cutAt > 0)
        {
            result[index++] = str.substr(0, cutAt);
//            result.push_back(str.substr(0, cutAt));
        }
        str = str.substr(cutAt + 1);
    }
    if(str.length() > 0)
    {
        result[index++] = str;
 //       result.push_back(str);
    }
}



void
p (string str, Json::Reader reader, Json::Value j)
{

  /*
  Json::Reader  reader;
  Json::Value j;
  */

//  std::string json  = "{\"data\":{\"data1\":\"abc\",\"data2\":\"def\"},\"message\":\"foo\",\"version\":1}";

  reader.parse ( str, j );
  string dateInfo[6];
  split( j["created_at"].asString() , " ", dateInfo );

  cout << j [ "user"]["id"].asInt64();
  cout << ",";
  cout << dateInfo[5] + "-" + month[dateInfo[1]] + "-" + dateInfo[2] + "+" + dateInfo[3] + ",";
  cout << j [ "id" ].asInt64() << endl;
}

int
main (int argc, char *argv[])
{

  string str;
month["Jan"]  = "01";
month["Feb"]  = "02";
month["Mar"]  = "03";
month["Apr"]  = "04";
month["May"]  = "05";
month["Jun"]  = "06";
month["Jul"]  = "07";
month["Aug"]  = "08";
month["Sep"]  = "09";
month["Oct"]  = "10";
month["Nov"]  = "11";
month["Dec"]  = "12";
  Json::Reader reader;
  Json::Value j;
  while(cin){
    getline(cin,str);
    p(str, reader, j);
  }
//  p ();


  exit (EXIT_SUCCESS);
}
