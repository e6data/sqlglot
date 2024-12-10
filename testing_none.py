car = {
    "brand": "Ford",
    "model": "Mustang",
    "year": ""
}

x = car.get("year1")

if x is None:
    print("getting noneee..")
print(type(x))
