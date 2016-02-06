import {Component} from 'angular2/core';

@Component({
	selector: 'twitter-timeline',
	template: `<div id="twitter-timeline">
            <a class="twitter-timeline"  href="https://twitter.com/puresoltech" data-widget-id="696031662912700417">Tweets by @puresoltech</a>
            <script>!function(d,s,id){var js,fjs=d.getElementsByTagName(s)[0],p=/^http:/.test(d.location)?'http':'https';if(!d.getElementById(id)){js=d.createElement(s);js.id=id;js.src=p+"://platform.twitter.com/widgets.js";fjs.parentNode.insertBefore(js,fjs);}}(document,"script","twitter-wjs");</script>
	    </div>
        `  
})
export class TwitterTimelineComponent {
  ngAfterContentInit(){
    twttr.widgets.load(document.getElementById("container"));
  }
}
