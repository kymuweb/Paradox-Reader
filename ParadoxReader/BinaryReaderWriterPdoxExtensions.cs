using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace ParadoxReader
{
    public static class BinaryReaderWriterPdoxExtensions
    {

        //public static string ReadBytesIntoBase64String(this BinaryReader reader, int count, bool returnNullInsteadOfThrow = true)
        //{
        //    string ret = null; // string.Empty;

        //    try
        //    {
        //        var buff = reader.ReadBytes(count);
        //        if ((buff?.Length ?? 0) > 0)
        //        {
        //            ret = Convert.ToBase64String(buff);
        //        }
        //        else
        //        {
        //            throw new Exception("Could not read bytes.");
        //        }
        //    }
        //    catch
        //    {
        //        if(returnNullInsteadOfThrow)
        //        {
        //            ret = null;
        //        }
        //        else //(Exception ex)
        //        {
        //            throw;
        //        }
        //    }

        //    return ret;
        //}



        /// <summary>
        /// Get the decimal value of the binary coded decimal (bytes).
        /// </summary>
        /// <param name="reader">Binary (byte) reader</param>
        /// <param name="bCDDecLen">Number of decimal places, often 2</param>
        /// <param name="bCDDataLen">Number of bytes, 17 for BDE</param>
        /// <param name="checkBCDDecLen">Check to make sure the decimal places match what is encoded in BCD</param>
        /// <param name="avoidThrow">Avoid throwing exception and instead return decimal.minvalue</param>
        /// <returns>Decimal value of the binary coded decimal</returns>
        /// <exception cref="Exception">Decimal length doesn't match, or couldn't parse the BCD string value.</exception>
        public static decimal ReadPdoxBCD(this BinaryReader reader, int bCDDecLen = 2, int bCDDataLen = 17, bool checkBCDDecLen = false, bool avoidThrow = true)
        {

            var ret = decimal.MinValue;

            var readerInitPos = reader.BaseStream.Position;

            try
            {

                const byte ZERO =           0b00000000; // 0x00;
                const byte FIFTEEN =        0b00001111; // 0x0f;
                const byte SIXTYTHREE =     0b00111111; // 0x3f;
                const byte ONETWENTYEIGHT = 0b10000000; // 0x80;
                const char ZEROC = '0';
                const char PERIODC = '.';

                string decimalDelimiter = "" + PERIODC; // TODO: get locale decimial delimiter

                var retStr = "";
                byte sign;
                byte nibble;
                bool leadingZero = true;
                int nibblesLen = bCDDataLen * 2;
                int nibblesIter = 0;

                byte currByte = ZERO;


                // Firstly start by reading the first byte, which contains the sign and decimal size.

                currByte = reader.ReadByte();

                if ((currByte & ONETWENTYEIGHT) > 0) // Positive
                {
                    sign = ZERO;
                }
                else // Negative
                {
                    sign = FIFTEEN;
                    retStr += "-";
                }
                int decLen = currByte & SIXTYTHREE; // The encoded size of the decimal component of this BCD
                if (checkBCDDecLen && (decLen != bCDDecLen)) // Check that the encoded size matches what we expect
                {
                    //return decimal.MinValue;
                    throw new Exception("BCD decimal length does not match expected value.");
                }


                // Now we get the chars before the decimal.

                for (nibblesIter = 2; nibblesIter < (nibblesLen - decLen); nibblesIter++)
                {
                    if ((nibblesIter % 2) > 0) // Odd
                    {
                        nibble = (byte)(currByte & FIFTEEN);
                    }
                    else
                    {
                        currByte = reader.ReadByte();
                        nibble = (byte)((currByte >> 4) & FIFTEEN);
                    }

                    int nibbleSigned = (nibble ^ sign);

                    if (leadingZero && (nibbleSigned > 0)) // We've found a nibble value more than zero, so turn off the leading zero flag.
                    {
                        leadingZero = false;
                    }
                    if (!leadingZero)
                    {
                        char nibbleChar = (char)(nibbleSigned + ZEROC);

                        retStr += nibbleChar;
                    }
                }

                // Did we have a leading zero? (I.e. no leading other chars?)

                if (leadingZero)
                {
                    retStr += ZEROC;
                }
                retStr += decimalDelimiter;

                // Now we get the chars after the decimal.

                for (; nibblesIter < nibblesLen; nibblesIter++)
                {
                    if ((nibblesIter % 2) > 0) // Odd
                    {
                        nibble = (byte)(currByte & FIFTEEN);
                    }
                    else
                    {
                        currByte = reader.ReadByte();
                        nibble = (byte)((currByte >> 4) & FIFTEEN);
                    }

                    int nibbleSigned = (nibble ^ sign);
                    char nibbleChar = (char)(nibbleSigned + ZEROC);

                    retStr += nibbleChar;
                }

                var parsed = decimal.TryParse(retStr, out ret);

                if (!parsed)
                {
                    throw new Exception("Could not parse BCD: '" + retStr + "'");
                }

            }
            catch //(Exception ex)
            {
                if(avoidThrow)
                {
                    ret = decimal.MinValue;
                }
                else
                {
                    throw;
                }
            }
            finally
            {
                try
                {
                    var postReadPos = reader.BaseStream.Position;
                    if (postReadPos != readerInitPos + bCDDataLen)
                    {
                        reader.BaseStream.Position = readerInitPos + bCDDataLen; // Reset the position to the end of the BCD data.
                    }
                }
                catch // Ingore this catch??
                {
                    //throw;
                }
            }


            return ret;
        }

        public static void WritePdoxBCD(this BinaryWriter writer, decimal value, int bCDDecLen = 2, int bCDDataLen = 17)
        {
            var outputBuf = new byte[bCDDataLen];
            byte sign;
            string strValue = value.ToString($"F{bCDDecLen}", System.Globalization.CultureInfo.InvariantCulture);
            char[] valueChars = strValue.ToCharArray();
            int i, j;


            const byte ZERO =           0b00000000; // 0x00;
            const byte FIFTEEN =        0b00001111; // 0x0f;
            const byte SIXTYFOUR =      0b01000000; // 0x40;
            const byte ONENINETYTWO =   0b11000000; // 0xc0;
            const byte TWOFORTY =       0b11110000; // 0xf0;
            const byte TWOFIFTYFIVE =   0b11111111; // 0xff;

            const char PERIODC = '.';
            const char MINUSC = '-';
            const char ZEROC = '0';
            const char NINEC = '9';

            var bCDNibbleLen = bCDDataLen * 2;

            Array.Clear(outputBuf, 0, bCDDataLen);

            if (valueChars.Length > 0)
            {
                j = 0;
                if (valueChars[0] == MINUSC)
                {
                    outputBuf[0] = (byte)(SIXTYFOUR + bCDDecLen);
                    sign = FIFTEEN;
                    for (int k = 1; k < bCDDataLen; k++)
                        outputBuf[k] = TWOFIFTYFIVE;
                }
                else
                {
                    outputBuf[0] = (byte)(ONENINETYTWO + bCDDecLen);
                    sign = ZERO;
                }

                // Find decimal point
                int dppos = strValue.IndexOf(PERIODC);
                if (dppos < 0)
                    dppos = strValue.Length;

                // Write fractional part (after decimal)
                j = (dppos < strValue.Length) ? dppos + 1 : strValue.Length;
                i = 0;
                while (i < bCDDecLen && j < valueChars.Length)
                {
                    char c = valueChars[j];
                    if (c >= ZEROC && c <= NINEC)
                    {
                        int nibble = (c - ZEROC) ^ sign;
                        int index = (bCDNibbleLen - bCDDecLen + i) / 2;
                        if (((bCDNibbleLen - bCDDecLen + i) % 2) != 0)
                            outputBuf[index] = (byte)((outputBuf[index] & TWOFORTY) | nibble);
                        else
                            outputBuf[index] = (byte)((outputBuf[index] & FIFTEEN) | (nibble << 4));
                        i++;
                    }
                    j++;
                }

                // Write integer part (before decimal)
                j = dppos - 1;
                i = bCDNibbleLen - bCDDecLen - 1;
                while (i > 1 && j >= 0)
                {
                    char c = valueChars[j];
                    if (c >= ZEROC && c <= NINEC)
                    {
                        int nibble = (c - ZEROC) ^ sign;
                        int index = i / 2;
                        if ((i % 2) != 0)
                            outputBuf[index] = (byte)((outputBuf[index] & TWOFORTY) | nibble);
                        else
                            outputBuf[index] = (byte)((outputBuf[index] & FIFTEEN) | (nibble << 4));
                        i--;
                    }
                    j--;
                }
            }

            // Write the result to the stream
            writer.Write(outputBuf, 0, bCDDataLen);
        }

        /// <summary>
        /// Converts the given byte array for a number (int or short) field, handling the bit flipping and endianness as needed for Paradox files. If inverse is false, it converts from the Paradox file format to the standard format. If inverse is true, it converts from the standard format to the Paradox file format.
        /// </summary>
        /// <param name="inverse">False = Read mode, True = Write mode</param>
        private static void ConvertBytesForInteger(byte[] data, int length, bool inverse, int start = 0)
        {
            if (!inverse)
            {
                data[start] ^= 0x80; // Flips the first bit.
            }
            Array.Reverse(data, start, length);
            if (inverse)
            {
                data[start] ^= 0x80; // Flips the first bit.
            }
        }

        /// <summary>
        /// Converts the given byte array for a number (double) field, handling the bit flipping and endianness as needed for Paradox files. If inverse is false, it converts from the Paradox file format to the standard format. If inverse is true, it converts from the standard format to the Paradox file format.
        /// </summary>
        /// <param name="inverse">False = Read mode, True = Write mode</param>
        public static void ConvertBytesForDouble(byte[] data, int length, bool inverse, int start = 0)
        {

            if (inverse)
            {
                Array.Reverse(data, start, length);
            }

            if ((data[start] & 0x80) != (inverse ? 0x80 : 0)) // First byte has high bit that needs to be flipped
            {
                data[start] ^= 0x80; // Flip high bit
            }
            else if (data.Skip(start).Take(length).All(b => b == 0)) // All bytes zero
            {
                // Do nothing
            }
            else
            {
                // Invert all bits
                for (int i = 0; i < length; i++)
                {
                    data[start + i] = (byte)(~data[start + i]);
                }
            }

            if (!inverse)
            {
                Array.Reverse(data, start, length);
            }

        }


        public static string ReadPdoxString(this BinaryReader reader, int dataSize)
        {
            byte[] bytes = reader.ReadBytes(dataSize);
            int stringLength = Array.IndexOf(bytes, (byte)0);
            if (stringLength < 0) stringLength = dataSize; // No null found, use all bytes
            return Encoding.Default.GetString(bytes, 0, stringLength);
        }

        public static void WritePdoxString(this BinaryWriter writer, string value, int dataSize)
        {
            writer.Write((value ?? string.Empty).PadRight(dataSize, '\0').Substring(0, dataSize).ToCharArray());
        }

        public static short ReadPdoxShort(this BinaryReader reader, int dataSize)
        {
            return reader.ReadPdoxNum<short>(dataSize);
        }

        public static void WritePdoxShort(this BinaryWriter writer, short value, int dataSize)
        {
            writer.WritePdoxNum<short>(value, dataSize);
        }

        public static int ReadPdoxInt(this BinaryReader reader, int dataSize)
        {
            return reader.ReadPdoxNum<int>(dataSize);
        }

        public static void WritePdoxInt(this BinaryWriter writer, int value, int dataSize)
        {
            writer.WritePdoxNum<int>(value, dataSize);
        }

        public static double ReadPdoxDouble(this BinaryReader reader, int dataSize)
        {
            return reader.ReadPdoxNum<double>(dataSize);
        }

        public static void WritePdoxDouble(this BinaryWriter writer, double value, int dataSize)
        {
            writer.WritePdoxNum<double>(value, dataSize);
        }

        public static DateTime ReadPdoxDate(this BinaryReader reader, int dataSize)
        {
            var days = reader.ReadPdoxInt(dataSize);
            return new DateTime(1, 1, 1).AddDays(days > 0 ? days - 1 : 0);
        }

        public static void WritePdoxDate(this BinaryWriter writer, DateTime value, int dataSize)
        {
            var days = (int)((value.Date - DateTime.MinValue).TotalDays);
            days = (days < 0) ? 0 : days + 1; // Paradox dates are 1-based, and have no representation for dates before 0001-01-01, so we set those to 0.
            writer.WritePdoxInt(days, dataSize);
        }

        public static TimeSpan ReadPdoxTime(this BinaryReader reader, int dataSize)
        {
            var msInt = reader.ReadPdoxInt(dataSize);
            return TimeSpan.FromMilliseconds(msInt >= 0 ? msInt : 0);
        }

        public static void WritePdoxTime(this BinaryWriter writer, TimeSpan value, int dataSize)
        {
            var msInt = (int)(value.TotalMilliseconds);
            msInt = (msInt < 0) ? 0 : msInt;
            writer.WritePdoxInt(msInt, dataSize);
        }

        public static DateTime ReadPdoxTimestamp(this BinaryReader reader, int dataSize)
        {
            var msDbl = reader.ReadPdoxDouble(dataSize);
            // TODO: Handle too large a value?
            return new DateTime(1, 1, 1).AddMilliseconds(msDbl >= 0 ? msDbl : 0).AddDays(msDbl >= 86400000 ? -1 : 0);
        }

        public static void WritePdoxTimestamp(this BinaryWriter writer, DateTime value, int dataSize)
        {
            var msDbl = ((value - DateTime.MinValue).TotalMilliseconds);
            msDbl = (msDbl < 0) ? 0 : msDbl + 86400000; // Paradox dates are 1-based, and have no representation for dates before 0001-01-01, so we set those to 0.
            writer.WritePdoxDouble(msDbl, dataSize);
        }

        public static bool ReadPdoxBool(this BinaryReader reader, int dataSize)
        {
            var b = reader.ReadByte();
            var ret = b > 128;
            reader.BaseStream.Position += dataSize - 1; // We've already read one byte, so move the position to the end of the bool data.
            return ret;
        }

        public static void WritePdoxBool(this BinaryWriter writer, bool value, int dataSize)
        {
            writer.Write((byte)(value ? 129 : 128));
            writer.BaseStream.Position += dataSize - 1; // We've already written one byte, so move the position to the end of the bool data.
        }

        public static byte[] ReadPdoxBytes(this BinaryReader reader, int dataSize)
        {
            return reader.ReadBytes(dataSize);
        }

        public static void WritePdoxBytes(this BinaryWriter writer, byte[] value, int dataSize)
        {
            if (value == null)
            {
                value = new byte[dataSize];
            }
            else if (value.Length != dataSize)
            {
                var tmp = new byte[dataSize];
                Array.Copy(value, tmp, Math.Min(value.Length, dataSize));
                value = tmp;
            }
            writer.Write(value, 0, dataSize);
        }

        private static void CheckTDataSize<T>(int dataSize)
        {
            var sizeOfT = System.Runtime.InteropServices.Marshal.SizeOf(typeof(T));
            if (dataSize != sizeOfT) { throw new Exception($"Paradox field data size for {typeof(T).Name} should be {sizeOfT} bytes, but was {dataSize} bytes."); };
        }

        private static T ReadPdoxNum<T>(this BinaryReader reader, int dataSize) where T : struct
        {
            CheckTDataSize<T>(dataSize);
            var data = reader.ReadBytes(dataSize);
            switch(typeof(T))
                {
                case Type t when t == typeof(short):
                    ConvertBytesForInteger(data, dataSize, inverse: false);
                    return (T)(object)BitConverter.ToInt16(data, 0);
                case Type t when t == typeof(int):
                    ConvertBytesForInteger(data, dataSize, inverse: false);
                    return (T)(object)BitConverter.ToInt32(data, 0);
                case Type t when t == typeof(double):
                    ConvertBytesForDouble(data, dataSize, inverse: false);
                    return (T)(object)BitConverter.ToDouble(data, 0);
                default:
                    throw new Exception($"Unsupported type {typeof(T)}");
            }
        }

        private static void WritePdoxNum<T>(this BinaryWriter writer, T value, int dataSize) where T : struct
        {
            CheckTDataSize<T>(dataSize);
            byte[] data = null;
            switch (typeof(T))
            {
                case Type t when t == typeof(short):
                    data = data = BitConverter.GetBytes((short)(object)value);
                    ConvertBytesForInteger(data, dataSize, inverse: true);
                    break;
                case Type t when t == typeof(int):
                    data = data = BitConverter.GetBytes((int)(object)value);
                    ConvertBytesForInteger(data, dataSize, inverse: true);
                    break;
                case Type t when t == typeof(double):
                    data = data = BitConverter.GetBytes((double)(object)value);
                    ConvertBytesForDouble(data, dataSize, inverse: true);
                    break;
                default:
                    throw new Exception($"Unsupported type {typeof(T)}");
            }
            writer.Write(data, 0, dataSize);
        }

    }


}
