jQuery(window).load(function(){
    jQuery(".input_prompt").on('click', function(){jQuery(this).next().children(".input_area").slideToggle()});
    
    
    jQuery(".input_prompt:eq(0)").next().children(".input_area").slideToggle();
    jQuery(".input_prompt:eq(0)").html("Config:");
    
});
