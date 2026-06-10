#include <iostream>
using namespace std;

class myBox {
    public:
    
        static int val;
        int a;

        static void myFunc(){
            val++;
        }

        int add(myBox BoxA){
            myBox BoxB;
            BoxB.a = BoxA.a + BoxB.a;
            return BoxB.a;
        }
};

int myBox::val = 0;

int  main(){
    myBox b ;
    myBox::myFunc();
    cout<<b.val<<endl;
    myBox A;
    A.a = 10;
    myBox B;
    cout<<B.add(A)<<endl;

}