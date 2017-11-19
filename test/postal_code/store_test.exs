defmodule ElhexDelivery.PostalCode.StoreTest do
    use ExUnit.Case
    alias ElhexDelivery.PostalCode.Store
    
    test "get_geolocation" do
        Store.start_link
        {latitude, longitude} = Store.get_geolocation("94062")

        assert is_float(latitude)
        assert is_float(longitude)
    end
end