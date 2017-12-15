create or replace package dvd_cht as
  function dvd_cht_1(col_name in varchar2, get_length in number)
    return varchar2;
  function dvd_cht_2(col_name in varchar2, get_length in number)
    return varchar2;

end;
/
create or replace package body dvd_cht as

  v_ans      number(5);
  v_ans_trim number(5);
  v_str1     varchar(100);
  v_str2     varchar(100);

  function dvd_cht_1(col_name in varchar2, get_length in number)
    return varchar2 as

    --if length = trim(length)  -->ok
    --if length != trim(length) --> -1

  begin

      v_ans      := length(substrb(col_name, get_length - 1, 4));
      v_ans_trim := length(trim(substrb(col_name, get_length - 1, 4)));

      if (v_ans = v_ans_trim) then
        v_str1 := substrb(col_name, 1, get_length);

        v_str2 := trim(substrb(col_name, get_length + 2));

      elsif v_ans != v_ans_trim or v_ans_trim is null then
        v_str1 := substrb(col_name, 1, get_length - 3);
        v_str2 := trim(substrb(col_name, get_length));

      else
        v_str1 := substrb(col_name, 1, get_length);
        v_str2 := trim(substrb(col_name, get_length + 2));

      end if;
      return(v_str1);


  end dvd_cht_1;
  function dvd_cht_2(col_name in varchar2, get_length in number)
    return varchar2 as
  begin

      if (v_ans = v_ans_trim) then
        v_str1 := substrb(col_name, 1, get_length);

        v_str2 := trim(substrb(col_name, get_length + 2));

      elsif v_ans != v_ans_trim or v_ans_trim is null then
        v_str1 := substrb(col_name, 1, get_length - 2);
        v_str2 := trim(substrb(col_name, get_length));

      else
        v_str1 := substrb(col_name, 1, get_length);
        v_str2 := trim(substrb(col_name, get_length + 2));

      end if;
      return(v_str2);

  end dvd_cht_2;

end;
/
