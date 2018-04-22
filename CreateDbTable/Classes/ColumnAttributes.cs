using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CreateDbTable.Classes
{
    class ColumnAttributes
    {
        public ColumnAttributes(string name)
        {
            Name = name;
            DataType = "";
            Length = 0;
            Precision = 0;
        }

        public string Name { get; set; }
        public string DataType { get; set; }
        public int Length { get; set; }
        public int Precision { get; set; }

        public void UpdateColumnAttributes(string data)
        {
            //check for date, int, or decimal.  if not one of those, call it nvarchar.
            //once it is nvarchar, it will never change.

            //if this column is an nvarchar, skip over everything and just check the length to see if we need to make it larger
            if (DataType != "nvarchar")
            {
                DateTime date;
                if (DateTime.TryParse(data, out date))
                {
                    //this might have been a date but let's check to see if was some other type before.  if so, we will call
                    //this nvarchar, set the length and get out.
                    if ((DataType != "datetime") & (DataType.Length > 0))
                    {
                        DataType = "nvarchar";
                        Length = GetLength(data.Length, Length);
                    }
                    else
                    {
                        DataType = "datetime";
                    }
                }
                else
                {
                    int result;
                    if (int.TryParse(data, out result))
                    {
                        //checking to see if this datatype was something else before we want it to be an int
                        if ((DataType != "int") & (DataType.Length > 0))
                        {
                            DataType = "nvarchar";
                            Length = GetLength(data.Length, Length);
                        }
                        else
                        {
                            DataType = "int";
                        }
                    }
                    else
                    {
                        decimal decResult;
                        if (decimal.TryParse(data, out decResult))
                        {
                            //was this datatype something else before we tried to set it to decimal?  if so, it's nvarchar from now on
                            if ((DataType != "decimal") & (DataType.Length > 0))
                            {
                                DataType = "nvarchar";
                                Length = GetLength(data.Length, Length);
                            }
                            else
                            {
                                DataType = "decimal";
                                int decLength = (Convert.ToString((Math.Truncate(decResult)))).Length;
                                Length = GetLength(decLength, Length);
                                int precLength = (Convert.ToString(decResult % 1.0m)).Length;
                                Precision = GetLength(precLength, Precision);
                            }
                        }
                        else
                        {
                            DataType = "nvarchar";
                            Length = GetLength(data.Length, Length);
                            Precision = 0;
                        }
                    }
                }
            }
            else
            {
                //this is an nvarchar field.  just check the length and get out.
                Length = GetLength(data.Length, Length);
            }
        }

        private int GetLength(int length1, int length2)
        {
            if (length1 > length2)
            {
                return length1;
            }
            else
            {
                return length2;
            }
        }
    }
}
