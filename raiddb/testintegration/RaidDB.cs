using System;
using System.Runtime.InteropServices;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace testintegration
{
    class RaidDB : IDisposable
    {
        [DllImport("raiddb.dll")]
        public static extern IntPtr CreateRaidDB(string conn1, string container1, string conn2, string container2);

        [DllImport("raiddb.dll")]
        public static extern void DeleteRaidDB(IntPtr raiddb);

        [DllImport("raiddb.dll")]
        public static extern int Open(IntPtr raiddb, string dbname);

        [DllImport("raiddb.dll")]
        public static extern void Close(IntPtr raiddb);

        [DllImport("raiddb.dll")]
        public static extern void Flush(IntPtr raiddb);

        [DllImport("raiddb.dll")]
        public static extern int Add(IntPtr raiddb, int length, IntPtr[] keyptrs, int[] keylens, IntPtr[] valueptrs, int[] valuelens);

        [DllImport("raiddb.dll")]
        public static extern int Get(IntPtr raiddb, int length, IntPtr[] keyptrs, int[] keylens, out IntPtr valueptrs, out IntPtr valuelens);

        [DllImport("raiddb.dll")]
        public static extern int Seek(IntPtr raiddb, string keyprefix, out long token);

        [DllImport("raiddb.dll")]
        public static extern void CloseScanToken(IntPtr raiddb, long token);

        [DllImport("raiddb.dll")]
        public static extern int Scan(IntPtr raiddb, long token, int batchSize, out int length, out IntPtr keyptrs, out IntPtr keylens, out IntPtr valueptrs, out IntPtr valuelens);

        [DllImport("raiddb.dll")]
        public static extern int ScanPartialOrder(IntPtr raiddb, long token, int batchSize, out int length, out IntPtr keyptrs, out IntPtr keylens, out IntPtr valueptrs, out IntPtr valuelens);

        [DllImport("raiddb.dll")]
        public static extern void FreeScan(IntPtr keyptrs, IntPtr keylens, IntPtr valueptrs, IntPtr valuelens);

        private IntPtr raiddb_; 

        public RaidDB(string conn1, string container1, string conn2, string container2)
        {
            raiddb_ = CreateRaidDB(conn1, container1, conn2, container2);
        }

        public void Dispose()
        {
            Close(raiddb_);
            DeleteRaidDB(raiddb_);
        }

        public void open(string dbname)
        {
            Open(raiddb_, dbname);
        }

        public void Flush()
        {
            Flush(raiddb_);
        }
        public void Add(Tuple<byte[], byte[]>[] data)
        {
            GCHandle[] keyhandles = new GCHandle[data.Length];
            GCHandle[] valuehandles = new GCHandle[data.Length];
            IntPtr[] keyptrs = new IntPtr[keyhandles.Length];
            IntPtr[] valueptrs = new IntPtr[valuehandles.Length];
            int[] keylens = new int[keyptrs.Length];
            int[] valuelens = new int[valueptrs.Length];
            for (int i = 0; i < data.Length; i++)
            {
                keyhandles[i] = GCHandle.Alloc(data[i].Item1, GCHandleType.Pinned);
                keyptrs[i] = keyhandles[i].AddrOfPinnedObject();
                keylens[i] = data[i].Item1.Length;
                valuehandles[i] = GCHandle.Alloc(data[i].Item2, GCHandleType.Pinned);
                valueptrs[i] = valuehandles[i].AddrOfPinnedObject();
                valuelens[i] = data[i].Item2.Length;
            }
            Add(raiddb_, data.Length, keyptrs, keylens, valueptrs, valuelens);
            for (int i = 0; i < data.Length; i++)
            {
                keyhandles[i].Free();
                valuehandles[i].Free();
            }
        }

        public void Get(Tuple<byte[]>[] keys, out Tuple<byte[]>[] values)
        {
            GCHandle[] keyhandles = new GCHandle[keys.Length];
            IntPtr[] keyptrs = new IntPtr[keyhandles.Length];
            int[] keylens = new int[keyptrs.Length];
            for (int i = 0; i < keys.Length; i++)
            {
                keyhandles[i] = GCHandle.Alloc(keys[i].Item1, GCHandleType.Pinned);
                keyptrs[i] = keyhandles[i].AddrOfPinnedObject();
                keylens[i] = keys[i].Item1.Length;
            }
            IntPtr valueptrs;
            IntPtr valuelensptr;
            Get(raiddb_, keys.Length, keyptrs, keylens, out valueptrs, out valuelensptr);
            int[] valuelens = new int[keys.Length];
            Marshal.Copy(valuelensptr, valuelens, 0, keys.Length);
            IntPtr valueptr = valueptrs;
            values = new Tuple<byte[]>[keys.Length];
            for (int i = 0; i < keys.Length; i++)
            {
                byte[] v = new byte[valuelens[i]];
                Marshal.Copy(valueptr, v, 0, valuelens[i]);
                values[i] = new Tuple<byte[]>(v);
                valueptr = IntPtr.Add(valueptr, valuelens[i]);
            }
            for (int i = 0; i < keys.Length; i++)
            {
                keyhandles[i].Free();
            }
        }

        public void Seek(string prefix, out long token)
        {
            Seek(raiddb_, prefix, out token);
        }

        public void CloseScanToken(long token)
        {
            CloseScanToken(raiddb_, token);
        }

        public void Scan(long token, int batchSize, out Tuple<byte[], byte[]>[] data)
        {
            int length;
            IntPtr keyptrs;
            IntPtr keylensptr;
            IntPtr valueptrs;
            IntPtr valuelensptr;
            Scan(raiddb_, token, batchSize, out length, out keyptrs, out keylensptr, out valueptrs, out valuelensptr);
            int[] keylens = new int[length];
            int[] valuelens = new int[length];
            Marshal.Copy(keylensptr, keylens, 0, length);
            Marshal.Copy(valuelensptr, valuelens, 0, length);
            IntPtr kk = keyptrs;
            IntPtr vv = valueptrs;
            data = new Tuple<byte[], byte[]>[length];
            for (int i = 0; i < length; i++)
            {
                byte[] k = new byte[keylens[i]];
                Marshal.Copy(kk, k, 0, keylens[i]);
                byte[] v = new byte[valuelens[i]];
                Marshal.Copy(vv, v, 0, valuelens[i]);
                kk = IntPtr.Add(kk, keylens[i]);
                vv = IntPtr.Add(vv, valuelens[i]);
                data[i] = new Tuple<byte[], byte[]>(k, v);
            }
            FreeScan(keyptrs, keylensptr, valueptrs, valuelensptr);
        }

        public void ScanPartialOrder(long token, int batchSize, out Tuple<byte[], byte[]>[] data)
        {
            int length;
            IntPtr keyptrs;
            IntPtr keylensptr;
            IntPtr valueptrs;
            IntPtr valuelensptr;
            ScanPartialOrder(raiddb_, token, batchSize, out length, out keyptrs, out keylensptr, out valueptrs, out valuelensptr);
            int[] keylens = new int[length];
            int[] valuelens = new int[length];
            Marshal.Copy(keylensptr, keylens, 0, length);
            Marshal.Copy(valuelensptr, valuelens, 0, length);
            IntPtr kk = keyptrs;
            IntPtr vv = valueptrs;
            data = new Tuple<byte[], byte[]>[length];
            for (int i = 0; i < length; i++)
            {
                byte[] k = new byte[keylens[i]];
                Marshal.Copy(kk, k, 0, keylens[i]);
                byte[] v = new byte[valuelens[i]];
                Marshal.Copy(vv, v, 0, valuelens[i]);
                kk = IntPtr.Add(kk, keylens[i]);
                vv = IntPtr.Add(vv, valuelens[i]);
                data[i] = new Tuple<byte[], byte[]>(k, v);
            }
            FreeScan(keyptrs, keylensptr, valueptrs, valuelensptr);
        }
    }
}
