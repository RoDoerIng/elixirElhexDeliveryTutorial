defmodule ElhexDelivery.PostalCode.DataParser do
    @postal_codes_filepath "data/2016_Gaz_zcta_national.txt"

    def parse_data do
        [_header | data_rows] = File.read!(@postal_codes_filepath) |> String.split("\n")

        data_rows
        |> Stream.map(&String.split(&1, "\t"))
        |> Stream.filter(&data_row?(&1))        # get rid of empty lines
        |> Stream.map(&parse_data_columns(&1))
        |> Stream.map(&format_row(&1))
        |> Enum.into(%{})
    end

    # Checks if 'row' is a 7 element list
    defp data_row?(row) do
        case row do
            # [postal_code, _, _, _, _, latitude, longitude] -> true
            [_, _, _, _, _, _, _] -> true
            [_] -> false
        end                
    end

    defp parse_data_columns(row) do
        [postal_code, _, _, _, _, latitude, longitude] = row
        [postal_code, latitude, longitude]
    end

    defp parse_number(str) do
        str |> String.replace(" ", "") |> String.to_float
    end

    # format a 3 element list into a 2 element tuple
    # [postal_code, latitude, longitude] # => {postal_codes, {latitude, longitude}
    defp format_row([postal_code, latitude, longitude]) do
        latitude = parse_number(latitude)
        longitude = parse_number(longitude)
        {postal_code, {latitude, longitude}}
    end
end