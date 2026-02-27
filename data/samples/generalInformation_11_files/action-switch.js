//solution for deprecate toggle func in jQuery 3.3.1
$.fn.onSwitch = function (f1,f2) {
    return this.each(function () {
       var click = false;
       $(this).on('click',function () {
           if(click){
               click = false;
               return f2.apply(this);
           }
           click = true;
           return f1.apply(this);
       });
    });
};