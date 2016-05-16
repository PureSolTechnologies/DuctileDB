import {Component} from 'angular2/core';

declare var twttr: any;

@Component({
	selector: 'twitter-ductiledb-timeline',
	template:
`<div id="twitter-ductiledb-timeline">
  <a class="twitter-timeline"  href="https://twitter.com/hashtag/DuctileDB" data-widget-id="696343984327815168">#DuctileDB Tweets</a>
  <script>!function(d,s,id){var js,fjs=d.getElementsByTagName(s)[0],p=/^http:/.test(d.location)?'http':'https';if(!d.getElementById(id)){js=d.createElement(s);js.id=id;js.src=p+"://platform.twitter.com/widgets.js";fjs.parentNode.insertBefore(js,fjs);}}(document,"script","twitter-wjs");</script>
</div>`  
})
export class TwitterDuctileDBTimelineComponent {
  ngAfterContentInit(){
    twttr.widgets.load(document.getElementById("twitter-ductiledb-timeline"));
  }
}
