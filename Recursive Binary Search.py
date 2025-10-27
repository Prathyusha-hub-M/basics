def binary_search(arr, first, last, x):
    if last >= first:
        mid = (first + last)//2
        if x == arr[mid]:
            return mid
        elif x < arr[mid]:
            return binary_search(arr, first, mid -1, x)
        else :
            return binary_search(arr, mid +1, last, x)
    else:
        return -1
    
if __name__ == "__main__":
    arr=[2,3,4,10,40]
    x=10

    result = binary_search(arr,0,len(arr)-1,x)

    print(result)
