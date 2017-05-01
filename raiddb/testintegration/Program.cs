using System;
using System.Runtime.InteropServices;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace testintegration
{
   
    class Program
    {       
        static void Main(string[] args)
        {
            string conn1 = @"DefaultEndpointsProtocol=https;AccountName=xdbs0;AccountKey=ypXmqWa9WQsHveizCQBnG/WmFNYrNjNnXsbkwhCcO4Mf9ZZe/z8nWd1w6/lXFveT9K+kFAwM6Ri46uK9jLJFrg==;EndpointSuffix=core.windows.net";
            string container1 = "dbx1";
            string conn2 = @"DefaultEndpointsProtocol=https;AccountName=xdbs1;AccountKey=l7IHazBj2/AzSRLjyuxrO5xyBZtOp9NV3NYfPbZmfGeTTYiUd478+5L1+HahwVdgfoXcUifLsas6F8aIUC4wyg==;EndpointSuffix=core.windows.net";
            string container2 = "dbx1";
            RaidDB raiddb = new RaidDB(conn1, container1, conn2, container2);
            raiddb.open("dbx1/acme");
            int len = 10;
            Tuple<byte[], byte[]>[] data = new Tuple<byte[], byte[]>[len];
            for (int i = 0; i < len; i++)
            {
                data[i] = new Tuple<byte[], byte[]>(Encoding.ASCII.GetBytes("key1#" + i), Encoding.ASCII.GetBytes("value1#" + i));
            }
            raiddb.Add(data);
            Tuple<byte[], byte[]>[] data2 = new Tuple<byte[], byte[]>[len];
            for (int i = 0; i < len; i++)
            {
                data2[i] = new Tuple<byte[], byte[]>(Encoding.ASCII.GetBytes("key2#" + i), Encoding.ASCII.GetBytes("value2#" + i));
            }
            raiddb.Add(data2);
            Tuple<byte[]>[] keys = new Tuple<byte[]>[] { new Tuple<byte[]>(Encoding.ASCII.GetBytes("key1#" + 5)), new Tuple<byte[]>(Encoding.ASCII.GetBytes("key2#" + 5)) };
            Tuple<byte[]>[] values;
            
            raiddb.Get(keys, out values);
            for(int i = 0; i < values.Length; i++)
            {
                Console.WriteLine("get value: " + Encoding.ASCII.GetString(values[i].Item1));
            }
            raiddb.Flush();

            long token;
            string kprefix = "key";
            raiddb.Seek(kprefix, out token);
            Console.WriteLine("token:" + token);
            raiddb.Scan(token, 100, out data);
            for (int i = 0; i < data.Length; i++)
            {
                Console.WriteLine("get key: " + Encoding.ASCII.GetString(data[i].Item1));
                Console.WriteLine("get value: " + Encoding.ASCII.GetString(data[i].Item2));
            }
            raiddb.ScanPartialOrder(token, 100, out data);
            for (int i = 0; i < data.Length; i++)
            {
                Console.WriteLine("get key: " + Encoding.ASCII.GetString(data[i].Item1));
                Console.WriteLine("get value: " + Encoding.ASCII.GetString(data[i].Item2));
            }
            raiddb.CloseScanToken(token);
            raiddb.Dispose();
        }
    }
}
