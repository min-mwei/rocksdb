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
            Test0(args);
            Test1(args);
        }
        static void Test0(string[] args)
        {
            string conn1 = @"DefaultEndpointsProtocol=https;AccountName=xdbs0;AccountKey=ypXmqWa9WQsHveizCQBnG/WmFNYrNjNnXsbkwhCcO4Mf9ZZe/z8nWd1w6/lXFveT9K+kFAwM6Ri46uK9jLJFrg==;EndpointSuffix=core.windows.net";
            string container1 = "dbacmebin";
            string conn2 = @"DefaultEndpointsProtocol=https;AccountName=xdbs1;AccountKey=l7IHazBj2/AzSRLjyuxrO5xyBZtOp9NV3NYfPbZmfGeTTYiUd478+5L1+HahwVdgfoXcUifLsas6F8aIUC4wyg==;EndpointSuffix=core.windows.net";
            string container2 = "dbacmebin";
            RaidDB raiddb = new RaidDB(conn1, container1, conn2, container2);
            raiddb.open("dbacmebin/acme");
            long token;
            Tuple<byte[], byte[]>[] data = new Tuple<byte[], byte[]>[4];

            byte[] aa = new byte[14] { 155, 71, 202, 201, 86, 24, 255, 250, 10, 121, 99, 103, 69, 117 };

            byte[] bb = new byte[14] { 155, 71, 202, 201, 86, 24, 255, 250, 0, 121, 99, 103, 69, 117 };
            byte[] cc = new byte[14] { 155, 71, 202, 201, 86, 24, 255, 250, 20, 121, 99, 103, 69, 117 };
            byte[] dd = new byte[14] { 155, 71, 202, 201, 86, 24, 255, 250, 5, 121, 99, 103, 69, 117 };

            data[0] = new Tuple<byte[], byte[]>(aa, aa);
            data[1] = new Tuple<byte[], byte[]>(bb, bb);
            data[2] = new Tuple<byte[], byte[]>(cc, cc);
            data[3] = new Tuple<byte[], byte[]>(dd, dd);

            raiddb.Add(data);

            byte[] key1 = new byte[14] { 155, 71, 202, 201, 86, 24, 255, 250, 0, 0, 0, 0, 0, 0 };
            byte[] key2 = new byte[14] { 155, 71, 202, 201, 86, 24, 255, 250, 60, 255, 255, 255, 255, 255 };
            raiddb.Seek(key1, out token);
            Tuple<byte[], byte[]>[] data2;
            raiddb.Scan(token, key2, Int32.MaxValue, out data2);
            Console.WriteLine("data2 length:" + data2.Length);
        }
        static void Test1(string[] args)
        {
            string conn1 = @"DefaultEndpointsProtocol=https;AccountName=xdbs0;AccountKey=ypXmqWa9WQsHveizCQBnG/WmFNYrNjNnXsbkwhCcO4Mf9ZZe/z8nWd1w6/lXFveT9K+kFAwM6Ri46uK9jLJFrg==;EndpointSuffix=core.windows.net";
            string container1 = "dbacmestring";
            string conn2 = @"DefaultEndpointsProtocol=https;AccountName=xdbs1;AccountKey=l7IHazBj2/AzSRLjyuxrO5xyBZtOp9NV3NYfPbZmfGeTTYiUd478+5L1+HahwVdgfoXcUifLsas6F8aIUC4wyg==;EndpointSuffix=core.windows.net";
            string container2 = "dbacmestring";
            RaidDB raiddb = new RaidDB(conn1, container1, conn2, container2);
            raiddb.open("dbacmestring/acme");
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
            raiddb.Seek(Encoding.ASCII.GetBytes(kprefix), out token);
            Console.WriteLine("new token:" + token);
            raiddb.Scan(token, 100, out data);
            for (int i = 0; i < data.Length; i++)
            {
                Console.WriteLine("get key: " + Encoding.ASCII.GetString(data[i].Item1));
                Console.WriteLine("get value: " + Encoding.ASCII.GetString(data[i].Item2));
            }
            raiddb.CloseScanToken(token);
            Console.WriteLine("token:" + token);
            raiddb.Seek(Encoding.ASCII.GetBytes(kprefix), out token);
            Console.WriteLine("new token:" + token);
            raiddb.Scan(token, Encoding.ASCII.GetBytes("key2#6"), 100, out data);
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
