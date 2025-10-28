def find_element(arr, x, findfirst):
    low=0
    high=len(arr)-1

    ind =-1

    while low<=high:
        mid=(low + high)//2

        if x == arr[mid]:
            ind = mid
            if findfirst:
                
                high = mid -1
            else :
                
                low = mid + 1

        elif x < arr[mid]:
                
                high = mid - 1
        else:
                low = mid + 1

    return ind








def find(arr,x):


    first= find_element(arr,x,True)
    last= find_element(arr,x,False)
    res = [first, last]
    return res

if __name__ == '__main__':
     arr= [1,3,5,5,5,5,67,1233,125]

     x=5
     res = find(arr, x)
     print(res[0],res[1])